#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def _read_csv(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"missing file: {p}")
    return pd.read_csv(p, low_memory=False)


def _require_cols(df: pd.DataFrame, cols: list[str], label: str):
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} missing columns: {missing}\nfound: {df.columns.tolist()}")


def _dedupe_on_playerId(df: pd.DataFrame, label: str) -> pd.DataFrame:
    if df["playerId"].duplicated().any():
        # keep first occurrence deterministically
        df = df.sort_values("playerId").drop_duplicates("playerId", keep="first")
    return df


def _load_phase4_player_event_rates(path: str) -> pd.DataFrame:
    """
    Accepts either:
      - canonical form: player_id, team_id, ... (plus season_year, game_id)
      - phase form: playerId, teamId, ...
    Produces a per-player season aggregate with totals + derived shares.
    """
    df = _read_csv(path)

    # normalize column names
    rename = {}
    if "player_id" in df.columns and "playerId" not in df.columns:
        rename["player_id"] = "playerId"
    if "team_id" in df.columns and "teamId" not in df.columns:
        rename["team_id"] = "teamId"
    df = df.rename(columns=rename)

    _require_cols(df, ["playerId"], "phase4_player_event_rates")
    if "season_year" not in df.columns:
        df["season_year"] = pd.NA

    # numeric cols we expect (if missing, we’ll just ignore)
    sum_cols = [c for c in [
        "fga","fta","three_pa","tov","ast","reb","pf","stl","blk",
        "clutch_fga","trans_fga","minutes_est"
    ] if c in df.columns]

    # aggregate: per player season
    agg = df.groupby(["playerId"], as_index=False)[sum_cols].sum() if sum_cols else df[["playerId"]].drop_duplicates()

    # derived shares (safe)
    if "fga" in agg.columns:
        agg["clutch_fga_share"] = (agg.get("clutch_fga", 0) / agg["fga"]).where(agg["fga"] > 0, 0.0)
        agg["trans_fga_share"] = (agg.get("trans_fga", 0) / agg["fga"]).where(agg["fga"] > 0, 0.0)

    # rename to avoid collisions with phase2 shooting fga/fta/etc
    # these are "phase4_" totals (play-by-play derived)
    rename2 = {c: f"phase4_{c}" for c in agg.columns if c not in ("playerId",)}
    agg = agg.rename(columns=rename2)

    return agg


def main():
    ap = argparse.ArgumentParser(description="Build a single player feature mart by joining phase0..phase3 (+ optional phase4).")
    ap.add_argument("--phase0", required=True, help="phase0_players_index_YYYY.csv (canonical spine)")
    ap.add_argument("--phase1", required=False, help="phase1_players_workload_YYYY.csv")
    ap.add_argument("--phase2_shooting", required=False, help="phase2_players_shooting_YYYY.csv")
    ap.add_argument("--phase2_impact", required=False, help="phase2_impact_misc_YYYY_rekeyed.csv")
    ap.add_argument("--phase3_profile", required=False, help="phase3_player_shot_profile_YYYY_rekeyed.csv")
    ap.add_argument("--phase4_event_rates", required=False, help="phase4_player_event_rates_YYYY(_canonical).csv")
    ap.add_argument("--out", required=True, help="output csv path, e.g., derived/player_feature_mart_2025.csv")
    args = ap.parse_args()

    # ---- phase0 spine
    p0 = _read_csv(args.phase0)
    _require_cols(p0, ["playerId", "playerName", "teamId", "pos"], "phase0")
    p0["playerId"] = p0["playerId"].astype(str)
    p0 = _dedupe_on_playerId(p0, "phase0")

    mart = p0.copy()

    # ---- phase1 workload
    if args.phase1:
        p1 = _read_csv(args.phase1)
        _require_cols(p1, ["playerId"], "phase1")
        p1["playerId"] = p1["playerId"].astype(str)
        p1 = _dedupe_on_playerId(p1, "phase1")
        # prefix non-key cols to avoid collision
        p1 = p1.rename(columns={c: f"phase1_{c}" for c in p1.columns if c != "playerId"})
        mart = mart.merge(p1, on="playerId", how="left")

    # ---- phase2 shooting
    if args.phase2_shooting:
        p2s = _read_csv(args.phase2_shooting)
        _require_cols(p2s, ["playerId"], "phase2_shooting")
        p2s["playerId"] = p2s["playerId"].astype(str)
        p2s = _dedupe_on_playerId(p2s, "phase2_shooting")
        p2s = p2s.rename(columns={c: f"phase2shoot_{c}" for c in p2s.columns if c != "playerId"})
        mart = mart.merge(p2s, on="playerId", how="left")

    # ---- phase2 impact/misc
    if args.phase2_impact:
        p2i = _read_csv(args.phase2_impact)
        _require_cols(p2i, ["playerId"], "phase2_impact")
        p2i["playerId"] = p2i["playerId"].astype(str)

        # keep useful columns; drop noisy duplicates if present
        drop_cols = [c for c in ["oldPlayerId", "oldTeamId", "playerName", "pos", "teamId"] if c in p2i.columns]
        keep = [c for c in p2i.columns if c not in drop_cols]
        p2i = p2i[keep].copy()

        p2i = _dedupe_on_playerId(p2i, "phase2_impact")
        p2i = p2i.rename(columns={c: f"phase2imp_{c}" for c in p2i.columns if c != "playerId"})
        mart = mart.merge(p2i, on="playerId", how="left")

    # ---- phase3 shot profile
    if args.phase3_profile:
        p3 = _read_csv(args.phase3_profile)
        _require_cols(p3, ["playerId"], "phase3_profile")
        p3["playerId"] = p3["playerId"].astype(str)

        # drop duplicate identity cols from phase3
        drop_cols = [c for c in ["playerName", "teamId", "pos", "age"] if c in p3.columns]
        keep = [c for c in p3.columns if c not in drop_cols]
        p3 = p3[keep].copy()

        p3 = _dedupe_on_playerId(p3, "phase3_profile")
        p3 = p3.rename(columns={c: f"phase3_{c}" for c in p3.columns if c != "playerId"})
        mart = mart.merge(p3, on="playerId", how="left")

    # ---- optional phase4 pbp-derived per-player totals
    if args.phase4_event_rates:
        p4 = _load_phase4_player_event_rates(args.phase4_event_rates)
        p4["playerId"] = p4["playerId"].astype(str)
        p4 = _dedupe_on_playerId(p4, "phase4_event_rates")
        mart = mart.merge(p4, on="playerId", how="left")

    # ---- quick sanity columns
    mart["season_year"] = (
        mart.get("seasonKey")
        if "seasonKey" in mart.columns
        else pd.NA
    )

    # ---- write
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    mart.to_csv(out_path, index=False)

    # ---- report
    print("wrote:", out_path)
    print("rows:", len(mart), "cols:", mart.shape[1])
    missing_phase1 = mart["phase1_g"].isna().sum() if "phase1_g" in mart.columns else "n/a"
    print("missing phase1 rows:", missing_phase1)


if __name__ == "__main__":
    main()
