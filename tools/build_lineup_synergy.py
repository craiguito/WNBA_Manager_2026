#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def safe_div(a, b, default=0.0):
    return a / b if b and b != 0 else default


def make_lineup_key(row) -> str:
    # order-independent lineup key
    players = [row["p1"], row["p2"], row["p3"], row["p4"], row["p5"]]
    players = [str(x) for x in players if pd.notna(x) and str(x).strip() != ""]
    players_sorted = sorted(players)
    return "|".join(players_sorted)


def load_team_poss_per_min(team_style_csv: Path) -> dict[str, float]:
    """
    Estimate team possessions per minute from season totals.
    poss ≈ FGA + 0.44*FTA + TOV  (OREB not available here; good enough for pace)
    poss_per_min ≈ poss / minutes_est
    """
    ts = pd.read_csv(team_style_csv, low_memory=False)

    needed = {"team_id", "minutes_est", "fga", "fta", "tov"}
    missing = needed - set(ts.columns)
    if missing:
        raise RuntimeError(f"team_style missing {missing}. columns: {ts.columns.tolist()}")

    ts = ts.copy()
    ts["team_id"] = ts["team_id"].astype(str)
    ts["minutes_est"] = pd.to_numeric(ts["minutes_est"], errors="coerce").fillna(0.0)
    ts["fga"] = pd.to_numeric(ts["fga"], errors="coerce").fillna(0.0)
    ts["fta"] = pd.to_numeric(ts["fta"], errors="coerce").fillna(0.0)
    ts["tov"] = pd.to_numeric(ts["tov"], errors="coerce").fillna(0.0)

    ts["poss_est"] = ts["fga"] + 0.44 * ts["fta"] + ts["tov"]
    ts["poss_per_min"] = ts.apply(lambda r: safe_div(r["poss_est"], r["minutes_est"], 0.0), axis=1)

    # if duplicates, take max minutes_est row
    ts = ts.sort_values("minutes_est", ascending=False).drop_duplicates("team_id", keep="first")
    return dict(zip(ts["team_id"], ts["poss_per_min"]))


def main():
    ap = argparse.ArgumentParser(description="Build lineup synergy table from lineup stints.")
    ap.add_argument("--stints", required=True, help="phase4_lineup_stints_2025_canonical.csv")
    ap.add_argument("--team-style", required=True, help="phase4_team_style_2025_canonical.csv (for possessions estimate)")
    ap.add_argument("--out", required=True, help="Output CSV path, e.g. derived/phase4_lineup_synergy_2025.csv")
    ap.add_argument("--min-minutes", type=float, default=10.0, help="Minimum minutes together to keep a lineup")
    ap.add_argument("--top-n-per-team", type=int, default=0, help="If >0, also write a top-N file per team next to out")
    args = ap.parse_args()

    st = pd.read_csv(args.stints, low_memory=False)

    needed = {"season_year", "game_id", "team_id", "side", "duration_s", "points_for", "points_against", "p1", "p2", "p3", "p4", "p5"}
    missing = needed - set(st.columns)
    if missing:
        raise RuntimeError(f"stints missing {missing}. columns: {st.columns.tolist()}")

    st = st.copy()
    st["team_id"] = st["team_id"].astype(str)

    # numeric cleanup
    st["duration_s"] = pd.to_numeric(st["duration_s"], errors="coerce").fillna(0.0)
    st["minutes"] = st["duration_s"] / 60.0
    st["points_for"] = pd.to_numeric(st["points_for"], errors="coerce").fillna(0.0)
    st["points_against"] = pd.to_numeric(st["points_against"], errors="coerce").fillna(0.0)

    # lineup key
    st["lineup_key"] = st.apply(make_lineup_key, axis=1)
    st = st[st["lineup_key"].str.len() > 0].copy()

    # possessions estimate using team style pace
    poss_per_min = load_team_poss_per_min(Path(args.team_style))
    st["team_poss_per_min"] = st["team_id"].map(poss_per_min).fillna(0.0)
    st["poss_est"] = st["team_poss_per_min"] * st["minutes"]

    # aggregate by team + lineup
    grp_cols = ["season_year", "team_id", "lineup_key"]
    agg = st.groupby(grp_cols, as_index=False).agg(
        minutes=("minutes", "sum"),
        stints=("minutes", "count"),
        points_for=("points_for", "sum"),
        points_against=("points_against", "sum"),
        poss_est=("poss_est", "sum"),
    )

    agg["net_points"] = agg["points_for"] - agg["points_against"]

    # per-40 minute rates
    agg["off_per40"] = agg.apply(lambda r: safe_div(r["points_for"], r["minutes"], 0.0) * 40.0, axis=1)
    agg["def_per40"] = agg.apply(lambda r: safe_div(r["points_against"], r["minutes"], 0.0) * 40.0, axis=1)
    agg["net_per40"] = agg.apply(lambda r: safe_div(r["net_points"], r["minutes"], 0.0) * 40.0, axis=1)

    # per-100 possession ratings (if poss_est present)
    agg["off_rating"] = agg.apply(lambda r: safe_div(r["points_for"], r["poss_est"], 0.0) * 100.0, axis=1)
    agg["def_rating"] = agg.apply(lambda r: safe_div(r["points_against"], r["poss_est"], 0.0) * 100.0, axis=1)
    agg["net_rating"] = agg["off_rating"] - agg["def_rating"]

    # pace proxy: possessions per 40
    agg["pace_poss_per40"] = agg.apply(lambda r: safe_div(r["poss_est"], r["minutes"], 0.0) * 40.0, axis=1)

    # split lineup_key back into players for readability
    players_split = agg["lineup_key"].str.split("|", expand=True)
    for i in range(5):
        col = f"p{i+1}"
        agg[col] = players_split[i] if i in players_split.columns else pd.NA

    # filter small samples
    agg = agg[agg["minutes"] >= args.min_minutes].copy()

    # sort: best chemistry = net_rating then net_per40 then minutes
    agg = agg.sort_values(["team_id", "net_rating", "net_per40", "minutes"], ascending=[True, False, False, False])

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cols = [
        "season_year","team_id","p1","p2","p3","p4","p5",
        "minutes","stints","points_for","points_against","net_points",
        "poss_est","pace_poss_per40",
        "off_per40","def_per40","net_per40",
        "off_rating","def_rating","net_rating",
        "lineup_key"
    ]
    agg[cols].to_csv(out_path, index=False)
    print(f"wrote {out_path}  (rows={len(agg)})")

    # optional: top N per team file
    if args.top_n_per_team and args.top_n_per_team > 0:
        topn = agg.groupby("team_id", as_index=False).head(args.top_n_per_team).copy()
        top_path = out_path.with_name(out_path.stem + f"_top{args.top_n_per_team}_per_team.csv")
        topn[cols].to_csv(top_path, index=False)
        print(f"wrote {top_path}  (rows={len(topn)})")


if __name__ == "__main__":
    main()
