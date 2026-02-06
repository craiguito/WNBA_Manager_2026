#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import pandas as pd

def safe_get(d: Optional[Dict[str, Any]], *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
    return cur if cur is not None else default

def as_bool_int(v):
    if v is True:
        return 1
    if v is False:
        return 0
    return None

def parse_game_file(path: Path):
    with path.open("r", encoding="utf-8") as f:
        g = json.load(f)

    game_id = g.get("id")

    # ---------- game row ----------
    game_row = {
        "source_file": path.name,
        "game_id": game_id,
        "sr_game_id": g.get("sr_id"),
        "reference_game_id": g.get("reference"),
        "status": g.get("status"),
        "coverage": g.get("coverage"),
        "scheduled_utc": g.get("scheduled"),
        "duration": g.get("duration"),
        "attendance": g.get("attendance"),
        "lead_changes": g.get("lead_changes"),
        "times_tied": g.get("times_tied"),
        "entry_mode": g.get("entry_mode"),
        "track_on_court": as_bool_int(g.get("track_on_court")),
        "season_id": safe_get(g, "season", "id"),
        "season_year": safe_get(g, "season", "year"),
        "season_type": safe_get(g, "season", "type"),
        "season_name": safe_get(g, "season", "name"),
        "home_team_id": safe_get(g, "home", "id"),
        "home_team_sr_id": safe_get(g, "home", "sr_id"),
        "home_team_ref": safe_get(g, "home", "reference"),
        "home_team_alias": safe_get(g, "home", "alias"),
        "home_team_name": safe_get(g, "home", "name"),
        "home_team_market": safe_get(g, "home", "market"),
        "home_points_final": safe_get(g, "home", "points"),
        "away_team_id": safe_get(g, "away", "id"),
        "away_team_sr_id": safe_get(g, "away", "sr_id"),
        "away_team_ref": safe_get(g, "away", "reference"),
        "away_team_alias": safe_get(g, "away", "alias"),
        "away_team_name": safe_get(g, "away", "name"),
        "away_team_market": safe_get(g, "away", "market"),
        "away_points_final": safe_get(g, "away", "points"),
        "tz_venue": safe_get(g, "time_zones", "venue"),
        "tz_home": safe_get(g, "time_zones", "home"),
        "tz_away": safe_get(g, "time_zones", "away"),
    }

    events_rows: List[Dict[str, Any]] = []
    event_stats_rows: List[Dict[str, Any]] = []
    lineup_rows: List[Dict[str, Any]] = []
    qualifier_rows: List[Dict[str, Any]] = []
    deleted_rows: List[Dict[str, Any]] = []

    for de in g.get("deleted_events", []) or []:
        deleted_rows.append({
            "game_id": game_id,
            "deleted_event_id": de.get("id")
        })

    for p in g.get("periods", []) or []:
        pnum = p.get("number")
        ptype = p.get("type")

        for ev in p.get("events", []) or []:
            event_id = ev.get("id")
            quals = ev.get("qualifiers") or []
            qualifiers_joined = "|".join(
                [str(q.get("qualifier")) for q in quals if isinstance(q, dict) and q.get("qualifier")]
            )

            events_rows.append({
                "game_id": game_id,
                "period_number": pnum,
                "period_type": ptype,
                "event_id": event_id,
                "event_number": ev.get("number"),
                "sequence": ev.get("sequence"),
                "created_utc": ev.get("created"),
                "updated_utc": ev.get("updated"),
                "wall_clock_utc": ev.get("wall_clock"),
                "clock": ev.get("clock"),
                "clock_decimal": ev.get("clock_decimal"),
                "event_type": ev.get("event_type"),
                "description": ev.get("description"),
                "home_points": ev.get("home_points"),
                "away_points": ev.get("away_points"),
                "attribution_team_id": safe_get(ev, "attribution", "id"),
                "attribution_team_sr_id": safe_get(ev, "attribution", "sr_id"),
                "attribution_team_name": safe_get(ev, "attribution", "name"),
                "possession_team_id": safe_get(ev, "possession", "id"),
                "possession_team_sr_id": safe_get(ev, "possession", "sr_id"),
                "possession_team_name": safe_get(ev, "possession", "name"),
                "turnover_type": ev.get("turnover_type"),
                "attempt": ev.get("attempt"),
                "duration": ev.get("duration"),
                "loc_x": safe_get(ev, "location", "coord_x"),
                "loc_y": safe_get(ev, "location", "coord_y"),
                "action_area": safe_get(ev, "location", "action_area"),
                "qualifiers_joined": qualifiers_joined,
                "source_file": path.name
            })

            # qualifiers normalized
            for qi, q in enumerate(quals):
                if not isinstance(q, dict):
                    continue
                qualifier_rows.append({
                    "game_id": game_id,
                    "event_id": event_id,
                    "qualifier_idx": qi,
                    "qualifier": q.get("qualifier"),
                    "value": q.get("value"),
                    "source_file": path.name
                })

            # stat objects normalized
            stats = ev.get("statistics") or []
            for si, s in enumerate(stats):
                if not isinstance(s, dict):
                    continue
                event_stats_rows.append({
                    "game_id": game_id,
                    "event_id": event_id,
                    "stat_idx": si,
                    "stat_type": s.get("type"),
                    "team_id": safe_get(s, "team", "id"),
                    "team_sr_id": safe_get(s, "team", "sr_id"),
                    "team_ref": safe_get(s, "team", "reference"),
                    "team_name": safe_get(s, "team", "name"),
                    "player_id": safe_get(s, "player", "id"),
                    "player_sr_id": safe_get(s, "player", "sr_id"),
                    "player_ref": safe_get(s, "player", "reference"),
                    "player_name": safe_get(s, "player", "full_name"),
                    "jersey_number": safe_get(s, "player", "jersey_number"),
                    "made": s.get("made"),
                    "points": s.get("points"),
                    "shot_type": s.get("shot_type"),
                    "shot_type_desc": s.get("shot_type_desc"),
                    "shot_distance": s.get("shot_distance"),
                    "rebound_type": s.get("rebound_type"),
                    "free_throw_type": s.get("free_throw_type"),
                    "three_point_shot": s.get("three_point_shot"),
                    "source_file": path.name
                })

            # on_court snapshots (home + away)
            oc = ev.get("on_court") or {}
            for side in ("home", "away"):
                team_blob = oc.get(side)
                if not isinstance(team_blob, dict):
                    continue
                lineup_team_id = team_blob.get("id")
                players = team_blob.get("players") or []
                for li, pl in enumerate(players):
                    if not isinstance(pl, dict):
                        continue
                    lineup_rows.append({
                        "game_id": game_id,
                        "event_id": event_id,
                        "period_number": pnum,
                        "event_number": ev.get("number"),
                        "sequence": ev.get("sequence"),
                        "side": side,
                        "lineup_slot_idx": li,
                        "lineup_team_id": lineup_team_id,
                        "lineup_team_sr_id": team_blob.get("sr_id"),
                        "lineup_team_ref": team_blob.get("reference"),
                        "lineup_team_name": team_blob.get("name"),
                        "player_id": pl.get("id"),
                        "player_sr_id": pl.get("sr_id"),
                        "player_ref": pl.get("reference"),
                        "player_name": pl.get("full_name"),
                        "jersey_number": pl.get("jersey_number"),
                        "source_file": path.name
                    })

    return game_row, events_rows, event_stats_rows, lineup_rows, qualifier_rows, deleted_rows

def main():
    ap = argparse.ArgumentParser(description="Extract Sportradar WNBA PBP JSONs into normalized CSV tables.")
    ap.add_argument("--in-dir", required=True, help="Folder containing game JSON files.")
    ap.add_argument("--out-dir", required=True, help="Folder to write CSV outputs.")
    ap.add_argument("--glob", default="*.json", help="File glob pattern (default: *.json).")
    args = ap.parse_args()

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = sorted(in_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files found in {in_dir} matching {args.glob}")

    games = []
    events = []
    stats = []
    lineups = []
    qualifiers = []
    deleted = []

    for i, fp in enumerate(files, 1):
        try:
            g, e, s, l, q, d = parse_game_file(fp)
            games.append(g)
            events.extend(e)
            stats.extend(s)
            lineups.extend(l)
            qualifiers.extend(q)
            deleted.extend(d)
        except Exception as ex:
            print(f"[WARN] Failed {fp.name}: {ex}")

        if i % 25 == 0 or i == len(files):
            print(f"processed {i}/{len(files)}")

    pd.DataFrame(games).to_csv(out_dir / "pbp_games.csv", index=False)
    pd.DataFrame(events).to_csv(out_dir / "pbp_events.csv", index=False)
    pd.DataFrame(stats).to_csv(out_dir / "pbp_event_stats.csv", index=False)
    pd.DataFrame(lineups).to_csv(out_dir / "pbp_lineups.csv", index=False)
    pd.DataFrame(qualifiers).to_csv(out_dir / "pbp_qualifiers.csv", index=False)
    pd.DataFrame(deleted).to_csv(out_dir / "pbp_deleted_events.csv", index=False)

    print("done")
    print(f"games={len(games)} events={len(events)} stats={len(stats)} lineups={len(lineups)} qualifiers={len(qualifiers)} deleted={len(deleted)}")

if __name__ == "__main__":
    main()
