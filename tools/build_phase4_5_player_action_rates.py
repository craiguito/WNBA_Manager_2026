#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import numpy as np
import pandas as pd


ACTIONS_SHOT = {"two_pa", "three_pa"}
ACTIONS_FT = {"fta"}
ACTIONS_AST = {"assist"}
ACTIONS_TOV = {"turnover"}
ACTIONS_FOUL_COMMIT = {"foul_committed"}
ACTIONS_FOUL_DRAWN = {"foul_drawn"}
ACTIONS_REB = {"orb", "drb"}

def load_minutes(phase1_csv: str | None) -> pd.DataFrame | None:
    if not phase1_csv:
        return None
    p = Path(phase1_csv)
    if not p.exists():
        return None
    df = pd.read_csv(p, low_memory=False)
    # expected cols: playerId, g, mpg
    if not {"playerId", "g", "mpg"} <= set(df.columns):
        raise RuntimeError(f"phase1 workload missing expected cols. got={df.columns.tolist()}")
    df = df.copy()
    df["playerId"] = df["playerId"].astype(str)
    df["g"] = pd.to_numeric(df["g"], errors="coerce").fillna(0)
    df["mpg"] = pd.to_numeric(df["mpg"], errors="coerce").fillna(0)
    df["minutes_est"] = df["g"] * df["mpg"]
    return df[["playerId", "minutes_est", "g", "mpg"]].rename(columns={"playerId": "player_id"})

def per36(x: float, minutes: float) -> float:
    if minutes and minutes > 0:
        return 36.0 * (x / minutes)
    return np.nan

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pbp-actions", required=True, help="derived/pbp_player_actions_2025.csv")
    ap.add_argument("--phase1-workload", default="", help="raw_data/phase1_players_workload_2025.csv (optional but recommended)")
    ap.add_argument("--out", default="derived/phase4_5_player_action_rates_2025.csv")
    args = ap.parse_args()

    df = pd.read_csv(args.pbp_actions, low_memory=False)
    df["player_id"] = df["player_id"].astype(str)
    df["season_year"] = df["season_year"].astype(str)

    # normalize action/result
    df["action"] = df["action"].astype(str).str.lower()
    df["result"] = df["result"].astype(str).str.lower()

    # basic makes
    df["is_made"] = (df["result"] == "made").astype(int)

    # counts by action (player-season)
    counts = df.pivot_table(
        index=["season_year", "player_id"],
        columns="action",
        values="game_id",
        aggfunc="count",
        fill_value=0
    ).reset_index()

    # derived core stats
    def col(c): return counts[c] if c in counts.columns else 0

    out = pd.DataFrame({
        "season_year": counts["season_year"],
        "player_id": counts["player_id"],

        "fga": col("two_pa") + col("three_pa"),
        "three_pa": col("three_pa"),
        "fta": col("fta"),

        "ast": col("assist"),
        "tov": col("turnover"),

        "pf_committed": col("foul_committed"),
        "pf_drawn": col("foul_drawn"),

        "orb": col("orb"),
        "drb": col("drb"),
    })

    # makes
    shots = df[df["action"].isin(ACTIONS_SHOT)].copy()
    makes = shots.groupby(["season_year", "player_id"])["is_made"].sum().reset_index().rename(columns={"is_made":"fgm"})
    out = out.merge(makes, on=["season_year","player_id"], how="left")
    out["fgm"] = out["fgm"].fillna(0).astype(int)

    # 3pt makes
    threes = df[df["action"] == "three_pa"].copy()
    threes_m = threes.groupby(["season_year","player_id"])["is_made"].sum().reset_index().rename(columns={"is_made":"three_pm"})
    out = out.merge(threes_m, on=["season_year","player_id"], how="left")
    out["three_pm"] = out["three_pm"].fillna(0).astype(int)

    # ft makes
    fts = df[df["action"] == "fta"].copy()
    ftm = fts.groupby(["season_year","player_id"])["is_made"].sum().reset_index().rename(columns={"is_made":"ftm"})
    out = out.merge(ftm, on=["season_year","player_id"], how="left")
    out["ftm"] = out["ftm"].fillna(0).astype(int)

    # rates
    out["fg_pct"] = np.where(out["fga"] > 0, out["fgm"] / out["fga"], np.nan)
    out["three_pct"] = np.where(out["three_pa"] > 0, out["three_pm"] / out["three_pa"], np.nan)
    out["ft_pct"] = np.where(out["fta"] > 0, out["ftm"] / out["fta"], np.nan)

    out["three_rate"] = np.where(out["fga"] > 0, out["three_pa"] / out["fga"], np.nan)
    out["fta_rate"] = np.where(out["fga"] > 0, out["fta"] / out["fga"], np.nan)
    out["tov_per_fga"] = np.where(out["fga"] > 0, out["tov"] / out["fga"], np.nan)
    out["ast_to_ratio"] = np.where(out["tov"] > 0, out["ast"] / out["tov"], np.nan)

    # per-36 using phase1 minutes
    mins = load_minutes(args.phase1_workload)
    if mins is not None:
        out = out.merge(mins, on="player_id", how="left")
        out["minutes_est"] = pd.to_numeric(out["minutes_est"], errors="coerce")
        for k in ["fga","three_pa","fta","ast","tov","pf_committed","pf_drawn","orb","drb"]:
            out[f"{k}_per36"] = out.apply(lambda r: per36(r[k], r["minutes_est"]), axis=1)
    else:
        out["minutes_est"] = np.nan

    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(args.out, index=False)
    print("wrote", args.out, "rows", len(out))


if __name__ == "__main__":
    main()
