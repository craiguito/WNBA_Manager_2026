#!/usr/bin/env python3
"""
Fix blank teamId in consolidated phase0 by recovering team from .bak files.
No re-scraping.

Reads:
- raw_data/phase0_players_index_2025.csv           (current, may have blank teamId)
- raw_data/phase0_players_index_2025.csv.bak       (original, contains team splits + TOT)
Optional (better team selection):
- raw_data/phase1_players_workload_2025.csv.bak    (original workload with per-team rows)

Writes:
- overwrites raw_data/phase0_players_index_2025.csv (creates .bak2 backup once)
"""

import argparse
import csv
import os
import re
import shutil
from typing import Dict, List, Tuple, Optional

RAW_DIR = "raw_data"

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def read_csv(path: str) -> Tuple[List[str], List[dict]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        return (r.fieldnames or []), list(r)

def write_csv(path: str, headers: List[str], rows: List[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow({h: row.get(h, "") for h in headers})

def backup2(path: str) -> None:
    bak2 = path + ".bak2"
    if not os.path.exists(bak2):
        shutil.copy2(path, bak2)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    args = ap.parse_args()

    p0 = os.path.join(RAW_DIR, f"phase0_players_index_{args.year}.csv")
    p0_bak = p0 + ".bak"
    p1_bak = os.path.join(RAW_DIR, f"phase1_players_workload_{args.year}.csv.bak")

    if not os.path.exists(p0):
        raise FileNotFoundError(p0)
    if not os.path.exists(p0_bak):
        raise FileNotFoundError(f"Missing backup: {p0_bak} (needed to recover teams)")

    # load backups
    p0b_h, p0b_rows = read_csv(p0_bak)

    # optional: phase1 backup for "most games" picking
    p1_stats = {}  # oldPlayerId -> games
    if os.path.exists(p1_bak):
        p1_h, p1_rows = read_csv(p1_bak)
        # try to find a games column
        games_col = "g" if "g" in p1_h else ("games" if "games" in p1_h else None)
        if games_col:
            for r in p1_rows:
                pid = (r.get("playerId") or "").strip()
                if not pid:
                    continue
                try:
                    g = float((r.get(games_col) or "").strip() or 0)
                except ValueError:
                    g = 0.0
                p1_stats[pid] = g

    # Build nameSlug -> bestTeamId from phase0 backup
    # Strategy:
    # - consider only non-TOT team rows
    # - if phase1 backup present, pick team row whose playerId has max games
    # - else pick first non-TOT team encountered
    best_team_by_slug: Dict[str, str] = {}
    best_games_by_slug: Dict[str, float] = {}

    for r in p0b_rows:
        name = (r.get("playerName") or "").strip()
        team = (r.get("teamId") or "").strip()
        old_pid = (r.get("playerId") or "").strip()
        if not name or not team:
            continue
        if team.upper() == "TOT":
            continue

        key = slugify(name)

        if p1_stats:
            g = p1_stats.get(old_pid, 0.0)
            if (key not in best_games_by_slug) or (g > best_games_by_slug[key]):
                best_games_by_slug[key] = g
                best_team_by_slug[key] = team
        else:
            # first non-TOT wins
            if key not in best_team_by_slug:
                best_team_by_slug[key] = team

    # Load current consolidated phase0
    p0_h, p0_rows = read_csv(p0)
    if "seasonKey" not in p0_h:
        p0_h.append("seasonKey")

    changed = 0
    missing = 0

    for r in p0_rows:
        name = (r.get("playerName") or "").strip()
        if not name:
            continue
        key = slugify(name)

        team = (r.get("teamId") or "").strip()
        if team == "":
            recovered = best_team_by_slug.get(key, "")
            if recovered:
                r["teamId"] = recovered
                team = recovered
                changed += 1
            else:
                missing += 1

        # rebuild seasonKey to match the now-known teamId
        if team:
            r["seasonKey"] = f"{key}|{team}"

    backup2(p0)
    write_csv(p0, ["playerId","playerName","teamId","pos","seasonKey"], p0_rows)

    print("✅ teamId recovery complete.")
    print(f"- filled teamId for {changed} players")
    if missing:
        print(f"⚠️ still missing teamId for {missing} players (no non-TOT team found in backup).")

if __name__ == "__main__":
    main()
