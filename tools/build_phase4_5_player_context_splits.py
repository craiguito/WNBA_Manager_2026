#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd


def is_clutch(period_number, clock_seconds) -> bool:
    try:
        p = int(period_number)
    except Exception:
        return False
    try:
        cs = int(clock_seconds)
    except Exception:
        return False
    return (p == 4 and cs <= 120) or (p >= 5)

def margin_bucket(m: float) -> str:
    if m is None or pd.isna(m):
        return "unknown"
    m = float(m)
    if m <= -10: return "trail_10plus"
    if m <= -4:  return "trail_4_9"
    if m < 0:    return "trail_1_3"
    if m == 0:   return "tied"
    if m < 4:    return "lead_1_3"
    if m < 10:   return "lead_4_9"
    return "lead_10plus"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pbp-actions", required=True, help="derived/pbp_player_actions_2025.csv")
    ap.add_argument("--phase1-workload", default="", help="raw_data/phase1_players_workload_2025.csv (optional for per36)")
    ap.add_argument("--out", default="derived/phase4_5_player_context_splits_2025.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.pbp_actions, low_memory=False)
    df["player_id"] = df["player_id"].astype(str)
    df["season_year"] = df["season_year"].astype(str)
    df["action"] = df["action"].astype(str).str.lower()
    df["result"] = df["result"].astype(str).str.lower()

    df["clock_seconds"] = pd.to_numeric(df["clock_seconds"], errors="coerce")
    df["period_number"] = pd.to_numeric(df["period_number"], errors="coerce")

    # margin_for_team might be blank/NaN if you didn’t include game_context earlier
    # we still split with margin_home fallback, but prefer margin_for_team when present.
    m_team = pd.to_numeric(df.get("margin_for_team"), errors="coerce")
    m_home = pd.to_numeric(df.get("margin_home"), errors="coerce")
    df["margin_used"] = np.where(~m_team.isna(), m_team, m_home)

    df["clutch"] = df.apply(lambda r: is_clutch(r["period_number"], r["clock_seconds"]), axis=1)
    df["bucket"] = df["margin_used"].apply(margin_bucket)

    # define what we care about
    df["is_fga"] = df["action"].isin(["two_pa","three_pa"]).astype(int)
    df["is_3pa"] = (df["action"] == "three_pa").astype(int)
    df["is_fta"] = (df["action"] == "fta").astype(int)
    df["is_ast"] = (df["action"] == "assist").astype(int)
    df["is_tov"] = (df["action"] == "turnover").astype(int)
    df["is_made"] = ((df["result"] == "made") & df["action"].isin(["two_pa","three_pa"])).astype(int)

    # clutch split
    clutch_agg = df.groupby(["season_year","player_id","clutch"], as_index=False).agg(
        fga=("is_fga","sum"),
        fgm=("is_made","sum"),
        three_pa=("is_3pa","sum"),
        fta=("is_fta","sum"),
        ast=("is_ast","sum"),
        tov=("is_tov","sum"),
    )
    clutch_agg["clutch"] = clutch_agg["clutch"].map({True:"clutch", False:"non_clutch"})

    clutch_wide = clutch_agg.pivot_table(
        index=["season_year","player_id"],
        columns="clutch",
        values=["fga","fgm","three_pa","fta","ast","tov"],
        aggfunc="sum",
        fill_value=0
    )
    clutch_wide.columns = [f"{a}_{b}" for a,b in clutch_wide.columns]
    clutch_wide = clutch_wide.reset_index()

    # margin bucket split (counts)
    bucket_agg = df.groupby(["season_year","player_id","bucket"], as_index=False).agg(
        fga=("is_fga","sum"),
        three_pa=("is_3pa","sum"),
        fta=("is_fta","sum"),
        tov=("is_tov","sum"),
        ast=("is_ast","sum"),
    )
    bucket_wide = bucket_agg.pivot_table(
        index=["season_year","player_id"],
        columns="bucket",
        values=["fga","three_pa","fta","tov","ast"],
        aggfunc="sum",
        fill_value=0
    )
    bucket_wide.columns = [f"{a}_{b}" for a,b in bucket_wide.columns]
    bucket_wide = bucket_wide.reset_index()

    # quarter split (fga + 3pa + tov)
    q = df[df["period_number"].between(1,4, inclusive="both")].copy()
    q["q"] = q["period_number"].astype(int)
    q_agg = q.groupby(["season_year","player_id","q"], as_index=False).agg(
        fga=("is_fga","sum"),
        three_pa=("is_3pa","sum"),
        tov=("is_tov","sum"),
        ast=("is_ast","sum"),
    )
    q_wide = q_agg.pivot_table(
        index=["season_year","player_id"],
        columns="q",
        values=["fga","three_pa","tov","ast"],
        aggfunc="sum",
        fill_value=0
    )
    q_wide.columns = [f"{a}_q{b}" for a,b in q_wide.columns]
    q_wide = q_wide.reset_index()

    out = clutch_wide.merge(bucket_wide, on=["season_year","player_id"], how="left").merge(q_wide, on=["season_year","player_id"], how="left")

    # add per-36 if phase1 available
    if args.phase1_workload:
        p1 = pd.read_csv(args.phase1_workload, low_memory=False)
        p1 = p1.rename(columns={"playerId":"player_id"}).copy()
        p1["player_id"] = p1["player_id"].astype(str)
        p1["g"] = pd.to_numeric(p1["g"], errors="coerce").fillna(0)
        p1["mpg"] = pd.to_numeric(p1["mpg"], errors="coerce").fillna(0)
        p1["minutes_est"] = p1["g"] * p1["mpg"]
        out = out.merge(p1[["player_id","minutes_est"]], on="player_id", how="left")

        for c in [c for c in out.columns if c.startswith(("fga_","three_pa_","fta_","tov_","ast_"))]:
            out[f"{c}_per36"] = np.where(out["minutes_est"] > 0, 36.0 * (out[c] / out["minutes_est"]), np.nan)

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print("wrote", args.out, "rows", len(out))


if __name__ == "__main__":
    main()
