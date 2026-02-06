#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def clock_to_sec(clock_str):
    if pd.isna(clock_str):
        return None
    try:
        m, s = str(clock_str).split(":")
        return int(m) * 60 + int(s)
    except Exception:
        return None


def period_len_sec(period_number: int) -> int:
    return 600 if period_number in (1, 2, 3, 4) else 300


def game_time_elapsed_sec(period_number: int, clock_str: str):
    rem = clock_to_sec(clock_str)
    if rem is None:
        return None
    plen = period_len_sec(period_number)
    elapsed_in_period = plen - rem
    base = sum(period_len_sec(p) for p in range(1, period_number))
    return base + elapsed_in_period


def is_clutch(period_number: int, clock_str: str, home_pts, away_pts) -> int:
    if period_number != 4:
        return 0
    rem = clock_to_sec(clock_str)
    if rem is None:
        return 0
    if home_pts is None or away_pts is None:
        return 0
    margin = abs(int(home_pts) - int(away_pts))
    return 1 if (rem <= 300 and margin <= 5) else 0


def normalize_lineup(ids):
    ids = [str(x) for x in ids if pd.notna(x)]
    ids = sorted(ids)
    return tuple(ids) if len(ids) == 5 else tuple(ids)


def main():
    ap = argparse.ArgumentParser(description="Build phase4 derived tables from normalized Sportradar PBP CSV tables.")
    ap.add_argument("--pbp-dir", required=True, help="Folder containing pbp_*.csv tables")
    ap.add_argument("--out-dir", required=True, help="Output folder for phase4 csvs")
    ap.add_argument("--year", default="2025")
    args = ap.parse_args()

    pbp_dir = Path(args.pbp_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    games = pd.read_csv(pbp_dir / "pbp_games.csv")
    events = pd.read_csv(pbp_dir / "pbp_events.csv")
    stats = pd.read_csv(pbp_dir / "pbp_event_stats.csv")
    lineups = pd.read_csv(pbp_dir / "pbp_lineups.csv")
    quals = pd.read_csv(pbp_dir / "pbp_qualifiers.csv")

    # groupers for per-game streaming
    events_g = events.groupby("game_id", sort=False)
    stats_g = stats.groupby("game_id", sort=False) if not stats.empty else {}
    lineups_g = lineups.groupby("game_id", sort=False) if not lineups.empty else {}
    quals_g = quals.groupby("game_id", sort=False) if not quals.empty else {}

    player_rows = []
    stint_rows = []
    game_rows = []
    team_style_rows = []

    for idx, g in games.iterrows():
        game_id = g["game_id"]
        if game_id not in events_g.groups:
            continue

        ev = events_g.get_group(game_id).copy()
        st = stats_g.get_group(game_id).copy() if hasattr(stats_g, "groups") and game_id in stats_g.groups else pd.DataFrame()
        lu = lineups_g.get_group(game_id).copy() if hasattr(lineups_g, "groups") and game_id in lineups_g.groups else pd.DataFrame()
        ql = quals_g.get_group(game_id).copy() if hasattr(quals_g, "groups") and game_id in quals_g.groups else pd.DataFrame()

        home_team_id = str(g.get("home_team_id")) if pd.notna(g.get("home_team_id")) else None
        away_team_id = str(g.get("away_team_id")) if pd.notna(g.get("away_team_id")) else None

        # time axis + clutch flag
        ev["period_number"] = pd.to_numeric(ev["period_number"], errors="coerce").fillna(0).astype(int)
        ev["t_elapsed"] = ev.apply(lambda r: game_time_elapsed_sec(int(r["period_number"]), r["clock"]), axis=1)
        ev["is_clutch"] = ev.apply(lambda r: is_clutch(int(r["period_number"]), r["clock"], r.get("home_points"), r.get("away_points")), axis=1)

        ev = ev.sort_values(["period_number", "event_number", "sequence"], kind="mergesort")

        # transition from qualifiers (basic)
        trans_ids = set()
        if not ql.empty and "qualifier" in ql.columns:
            trans_ids = set(
                ql.loc[ql["qualifier"].astype(str).str.contains("fastbreak", case=False, na=False), "event_id"].astype(str).tolist()
            )
        ev["event_id"] = ev["event_id"].astype(str)
        ev["is_transition"] = ev["event_id"].isin(trans_ids).astype(int)

        # =========================
        # 1) lineup stints per team
        # =========================
        if not lu.empty:
            lu2 = lu.dropna(subset=["event_id", "player_id"]).copy()
            lu2["event_id"] = lu2["event_id"].astype(str)
            lu2["player_id"] = lu2["player_id"].astype(str)

            lineup_by_event_side = (
                lu2.groupby(["event_id", "side"])["player_id"]
                .apply(lambda s: normalize_lineup(list(s)))
                .reset_index()
            )

            home_line = lineup_by_event_side[lineup_by_event_side["side"] == "home"][["event_id", "player_id"]].rename(columns={"player_id": "home_lineup"})
            away_line = lineup_by_event_side[lineup_by_event_side["side"] == "away"][["event_id", "player_id"]].rename(columns={"player_id": "away_lineup"})

            ev = ev.merge(home_line, on="event_id", how="left").merge(away_line, on="event_id", how="left")
            ev["home_lineup"] = ev["home_lineup"].ffill()
            ev["away_lineup"] = ev["away_lineup"].ffill()

            def emit_stints(side: str, team_id: str | None, lineup_col: str):
                nonlocal stint_rows
                if team_id is None:
                    return
                sub = ev.dropna(subset=[lineup_col, "t_elapsed"]).copy()
                if sub.empty:
                    return

                sub["_change"] = sub[lineup_col].ne(sub[lineup_col].shift(1))
                sub["_stint"] = sub["_change"].cumsum()

                for stint_id, sg in sub.groupby("_stint"):
                    start_t = float(sg["t_elapsed"].min())
                    end_t = float(sg["t_elapsed"].max())
                    duration_s = max(0.0, end_t - start_t)

                    start_home = sg.iloc[0].get("home_points")
                    start_away = sg.iloc[0].get("away_points")
                    end_home = sg.iloc[-1].get("home_points")
                    end_away = sg.iloc[-1].get("away_points")

                    pf = pa = None
                    if pd.notna(start_home) and pd.notna(start_away) and pd.notna(end_home) and pd.notna(end_away):
                        if side == "home":
                            pf = int(end_home - start_home)
                            pa = int(end_away - start_away)
                        else:
                            pf = int(end_away - start_away)
                            pa = int(end_home - start_home)

                    lineup = sg.iloc[0][lineup_col]
                    lineup_list = list(lineup) if isinstance(lineup, tuple) else []

                    stint_rows.append({
                        "season_year": args.year,
                        "game_id": game_id,
                        "team_id": team_id,
                        "side": side,
                        "stint_id": int(stint_id),
                        "start_t": start_t,
                        "end_t": end_t,
                        "duration_s": duration_s,
                        "points_for": pf,
                        "points_against": pa,
                        "p1": lineup_list[0] if len(lineup_list) > 0 else None,
                        "p2": lineup_list[1] if len(lineup_list) > 1 else None,
                        "p3": lineup_list[2] if len(lineup_list) > 2 else None,
                        "p4": lineup_list[3] if len(lineup_list) > 3 else None,
                        "p5": lineup_list[4] if len(lineup_list) > 4 else None,
                    })

            emit_stints("home", home_team_id, "home_lineup")
            emit_stints("away", away_team_id, "away_lineup")

        # =================================
        # 2) player event rates (per game)
        # =================================
        if not st.empty:
            st2 = st.dropna(subset=["player_id", "stat_type"]).copy()
            st2["event_id"] = st2["event_id"].astype(str)
            st2["player_id"] = st2["player_id"].astype(str)
            st2["team_id"] = st2["team_id"].astype(str)

            ev_ctx = ev[["event_id", "is_clutch", "is_transition"]].copy()
            st2 = st2.merge(ev_ctx, on="event_id", how="left")
            st2["is_clutch"] = st2["is_clutch"].fillna(0).astype(int)
            st2["is_transition"] = st2["is_transition"].fillna(0).astype(int)

            stat_type = st2["stat_type"].astype(str).str.lower()

            st2["is_fga"] = stat_type.str.contains("fieldgoal").astype(int)
            st2["is_fta"] = stat_type.str.contains("freethrow").astype(int)
            st2["is_tov"] = stat_type.str.contains("turnover").astype(int)
            st2["is_ast"] = stat_type.str.contains("assist").astype(int)
            st2["is_reb"] = stat_type.str.contains("rebound").astype(int)
            st2["is_pf"] = stat_type.str.contains("foul").astype(int)
            st2["is_stl"] = stat_type.str.contains("steal").astype(int)
            st2["is_blk"] = stat_type.str.contains("block").astype(int)

            if "three_point_shot" in st2.columns:
                st2["is_3pa"] = (st2["three_point_shot"].astype(str).str.lower() == "true").astype(int)
            else:
                st2["is_3pa"] = 0

            # minutes estimate from stints within this game
            minutes_by_player = None
            if stint_rows:
                stints_df = pd.DataFrame([r for r in stint_rows if r["game_id"] == game_id])
                if not stints_df.empty:
                    melted = stints_df.melt(
                        id_vars=["game_id", "team_id", "duration_s"],
                        value_vars=["p1", "p2", "p3", "p4", "p5"],
                        value_name="player_id"
                    ).dropna(subset=["player_id"])
                    melted["player_id"] = melted["player_id"].astype(str)
                    minutes_by_player = melted.groupby("player_id")["duration_s"].sum().reset_index()
                    minutes_by_player["minutes_est"] = minutes_by_player["duration_s"] / 60.0

            agg = st2.groupby(["player_id", "team_id"]).agg(
                fga=("is_fga", "sum"),
                fta=("is_fta", "sum"),
                three_pa=("is_3pa", "sum"),
                tov=("is_tov", "sum"),
                ast=("is_ast", "sum"),
                reb=("is_reb", "sum"),
                pf=("is_pf", "sum"),
                stl=("is_stl", "sum"),
                blk=("is_blk", "sum"),
                clutch_fga=("is_fga", lambda x: int(((x == 1) & (st2.loc[x.index, "is_clutch"] == 1)).sum())),
                trans_fga=("is_fga", lambda x: int(((x == 1) & (st2.loc[x.index, "is_transition"] == 1)).sum())),
            ).reset_index()

            if minutes_by_player is not None:
                agg = agg.merge(minutes_by_player[["player_id", "minutes_est"]], on="player_id", how="left")
            else:
                agg["minutes_est"] = pd.NA

            agg["season_year"] = args.year
            agg["game_id"] = game_id

            player_rows.extend(agg.to_dict("records"))

        # =========================
        # 3) game context
        # =========================
        game_rows.append({
            "season_year": args.year,
            "game_id": game_id,
            "home_team_id": home_team_id,
            "away_team_id": away_team_id,
            "home_points_final": g.get("home_points_final"),
            "away_points_final": g.get("away_points_final"),
            "lead_changes": g.get("lead_changes"),
            "times_tied": g.get("times_tied"),
            "events_count": int(len(ev)),
            "has_lineups": int(not lu.empty),
            "has_stats": int(not st.empty),
        })

        if (idx + 1) % 25 == 0:
            print(f"processed {idx+1}/{len(games)} games")

    # =========================
    # write outputs
    # =========================
    phase4_player = pd.DataFrame(player_rows)
    phase4_stints = pd.DataFrame(stint_rows)
    phase4_games = pd.DataFrame(game_rows)

    phase4_player.to_csv(out_dir / "phase4_player_event_rates_2025.csv", index=False)
    phase4_stints.to_csv(out_dir / "phase4_lineup_stints_2025.csv", index=False)
    phase4_games.to_csv(out_dir / "phase4_game_context_2025.csv", index=False)

    # =========================
    # team style (simple aggregate)
    # =========================
    if not phase4_player.empty:
        team_style = phase4_player.groupby("team_id", as_index=False).agg(
            games=("game_id", "nunique"),
            minutes_est=("minutes_est", "sum"),
            fga=("fga", "sum"),
            three_pa=("three_pa", "sum"),
            fta=("fta", "sum"),
            tov=("tov", "sum"),
            ast=("ast", "sum"),
        )
        # simple ratios
        team_style["three_rate"] = team_style["three_pa"] / team_style["fga"].replace({0: pd.NA})
        team_style["fta_rate"] = team_style["fta"] / team_style["fga"].replace({0: pd.NA})
        team_style["tov_per_36"] = team_style["tov"] / (team_style["minutes_est"].replace({0: pd.NA}) / 36.0)
        team_style["season_year"] = args.year
        team_style.to_csv(out_dir / "phase4_team_style_2025.csv", index=False)
    else:
        pd.DataFrame().to_csv(out_dir / "phase4_team_style_2025.csv", index=False)

    print("done")
    print("wrote:")
    print(" - phase4_player_event_rates_2025.csv")
    print(" - phase4_lineup_stints_2025.csv")
    print(" - phase4_game_context_2025.csv")
    print(" - phase4_team_style_2025.csv")


if __name__ == "__main__":
    main()
