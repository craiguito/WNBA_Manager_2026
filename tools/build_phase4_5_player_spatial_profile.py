#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd
import math


def zone_from_xy(hx: float, hy: float) -> str:
    """
    hx, hy: normalized half-court where hoop is (0,0)
    dist thresholds tuned to your Sportradar unit scale; adjust later if needed.
    """
    dist = math.sqrt(hx*hx + hy*hy)

    # 3pt boundary (roughly)
    is_three = dist >= 240

    if dist <= 60:
        return "rim"
    if dist <= 140:
        return "paint"
    if not is_three:
        return "mid"

    # 3 zones
    if abs(hx) >= 250:
        return "corner3"
    return "ab3"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pbp-actions", required=True, help="derived/pbp_player_actions_2025.csv")
    ap.add_argument("--out", default="derived/phase4_5_player_spatial_profile_2025.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.pbp_actions, low_memory=False)
    df["player_id"] = df["player_id"].astype(str)
    df["season_year"] = df["season_year"].astype(str)
    df["action"] = df["action"].astype(str).str.lower()
    df["result"] = df["result"].astype(str).str.lower()

    shots = df[df["action"].isin(["two_pa", "three_pa"])].copy()
    shots["hx"] = pd.to_numeric(shots["hx"], errors="coerce")
    shots["hy"] = pd.to_numeric(shots["hy"], errors="coerce")
    shots = shots.dropna(subset=["hx","hy"])

    shots["is_made"] = (shots["result"] == "made").astype(int)
    shots["shot_side"] = np.where(shots["hx"] < 0, "left", "right")
    shots["zone"] = shots.apply(lambda r: zone_from_xy(float(r["hx"]), float(r["hy"])), axis=1)
    shots["dist"] = np.sqrt(shots["hx"]**2 + shots["hy"]**2)

    # per-player aggregates
    g = shots.groupby(["season_year","player_id"], as_index=False).agg(
        fga=("zone","size"),
        fgm=("is_made","sum"),
        avg_dist=("dist","mean"),
        sd_dist=("dist","std"),
        avg_hx=("hx","mean"),
        avg_hy=("hy","mean"),
    )

    # zone attempts + makes
    zone_a = shots.pivot_table(index=["season_year","player_id"], columns="zone", values="game_id", aggfunc="count", fill_value=0)
    zone_m = shots.pivot_table(index=["season_year","player_id"], columns="zone", values="is_made", aggfunc="sum", fill_value=0)

    zone_a = zone_a.add_prefix("att_").reset_index()
    zone_m = zone_m.add_prefix("made_").reset_index()

    out = g.merge(zone_a, on=["season_year","player_id"], how="left").merge(zone_m, on=["season_year","player_id"], how="left")

    # fg% by zone + share by zone
    for z in ["rim","paint","mid","corner3","ab3"]:
        att = f"att_{z}"
        made = f"made_{z}"
        if att not in out.columns: out[att] = 0
        if made not in out.columns: out[made] = 0
        out[f"fg_{z}"] = np.where(out[att] > 0, out[made] / out[att], np.nan)
        out[f"share_{z}"] = np.where(out["fga"] > 0, out[att] / out["fga"], np.nan)

    # left/right bias (attempt share)
    side = shots.pivot_table(index=["season_year","player_id"], columns="shot_side", values="game_id", aggfunc="count", fill_value=0).reset_index()
    side.rename(columns={"left":"att_left","right":"att_right"}, inplace=True)
    out = out.merge(side, on=["season_year","player_id"], how="left")
    out["left_share"] = np.where(out["fga"] > 0, out["att_left"].fillna(0) / out["fga"], np.nan)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print("wrote", args.out, "rows", len(out))


if __name__ == "__main__":
    main()
