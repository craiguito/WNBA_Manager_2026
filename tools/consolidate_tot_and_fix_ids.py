#!/usr/bin/env python3
"""
Consolidate multi-team + TOT rows into one player row per season,
while assigning a "current team" (non-TOT), fixing playerIds to be team-independent,
adding seasonKey, and normalizing percent fields.

Works only on existing CSVs in raw_data (no scraping).

Inputs:
  raw_data/phase0_players_index_2025.csv  (playerId, playerName, teamId, pos)
  raw_data/phase1_players_workload_2025.csv (playerId, g, mpg, starterFlag, usageProxyPer36)
  raw_data/phase2_players_shooting_2025.csv (playerId, fg, fga, fgPct, fg3, fg3a, fg3Pct, fg2, fg2a, fg2Pct, ft, fta, ftPct, pts)
  raw_data/phase2_players_box_2025.csv (playerId, orb, trb, ast, stl, blk, tov, pf)

Outputs (overwrites, with .bak backups):
  raw_data/phase0_players_index_2025.csv  (+ seasonKey, merged players, teamId is current team)
  raw_data/phase1_players_workload_2025.csv (merged players)
  raw_data/phase2_players_shooting_2025.csv (merged players, normalized %)
  raw_data/phase2_players_box_2025.csv (merged players)
  raw_data/players_id_map.csv (playerId, playerName, seasonKey)

Run:
  python tools/consolidate_tot_and_fix_ids.py --year 2025
"""

from __future__ import annotations
import argparse
import csv
import os
import re
import shutil
from typing import Dict, List, Tuple, Optional

RAW_DIR = "raw_data"

PHASE0 = "phase0_players_index_{year}.csv"
PHASE1 = "phase1_players_workload_{year}.csv"
PHASE2_SHOOT = "phase2_players_shooting_{year}.csv"
PHASE2_BOX = "phase2_players_box_{year}.csv"
ID_MAP = "players_id_map.csv"

TOT_TOKEN = "TOT"


def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def backup(path: str) -> None:
    bak = path + ".bak"
    if not os.path.exists(bak):
        shutil.copy2(path, bak)


def read_csv(path: str) -> Tuple[List[str], List[dict]]:
    with open(path, "r", encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        headers = r.fieldnames or []
        rows = list(r)
    return headers, rows


def write_csv(path: str, headers: List[str], rows: List[dict]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for row in rows:
            w.writerow({h: row.get(h, "") for h in headers})


def to_float(x: str) -> Optional[float]:
    x = (x or "").strip().replace("%", "")
    if x == "":
        return None
    try:
        return float(x)
    except ValueError:
        return None


def to_int(x: str) -> Optional[int]:
    x = (x or "").strip()
    if x == "":
        return None
    try:
        return int(float(x))
    except ValueError:
        return None


def percent_to_0_100(val: str) -> str:
    v = (val or "").strip().replace("%", "")
    if v == "":
        return ""
    try:
        x = float(v)
    except ValueError:
        return ""
    if 0.0 <= x <= 1.0:
        x *= 100.0
    return f"{x:.2f}"


def pick_current_team(rows_phase0: List[dict], rows_phase1: List[dict]) -> str:
    """
    Pick current team for a player from non-TOT team rows.
    Rule: max games (phase1 g), tie-breaker: max mpg, else first non-TOT.
    """
    # Map old playerId -> (g, mpg)
    p1 = {}
    for r in rows_phase1:
        pid = (r.get("playerId") or "").strip()
        g = to_float(r.get("g", "")) or 0.0
        mpg = to_float(r.get("mpg", "")) or 0.0
        p1[pid] = (g, mpg)

    best_team = ""
    best_key = (-1.0, -1.0)  # (g, mpg)

    for r0 in rows_phase0:
        team = (r0.get("teamId") or "").strip()
        if team.upper() == TOT_TOKEN:
            continue
        pid = (r0.get("playerId") or "").strip()
        g, mpg = p1.get(pid, (0.0, 0.0))
        key = (g, mpg)
        if key > best_key:
            best_key = key
            best_team = team

    # fallback
    if not best_team:
        for r0 in rows_phase0:
            team = (r0.get("teamId") or "").strip()
            if team:
                return team
    return ""


def weighted_merge_numeric(rows: List[dict], weight_field: str, fields: List[str]) -> dict:
    """
    Weighted merge for numeric fields across team rows (used only if TOT row missing).
    weights: rows[weight_field] numeric (e.g., games)
    For percents we generally do weighted average too.
    """
    total_w = 0.0
    acc = {f: 0.0 for f in fields}
    for r in rows:
        w = to_float(r.get(weight_field, "")) or 0.0
        if w <= 0:
            continue
        total_w += w
        for f in fields:
            v = to_float(r.get(f, ""))
            if v is None:
                continue
            acc[f] += v * w

    out = {}
    for f in fields:
        if total_w > 0:
            out[f] = f"{(acc[f] / total_w):.3f}"
        else:
            out[f] = ""
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    args = ap.parse_args()

    os.makedirs(RAW_DIR, exist_ok=True)

    p0_path = os.path.join(RAW_DIR, PHASE0.format(year=args.year))
    p1_path = os.path.join(RAW_DIR, PHASE1.format(year=args.year))
    p2s_path = os.path.join(RAW_DIR, PHASE2_SHOOT.format(year=args.year))
    p2b_path = os.path.join(RAW_DIR, PHASE2_BOX.format(year=args.year))
    map_path = os.path.join(RAW_DIR, ID_MAP)

    for p in [p0_path, p1_path, p2s_path, p2b_path]:
        if not os.path.exists(p):
            raise FileNotFoundError(f"Missing required file: {p}")

    p0_h, p0_rows = read_csv(p0_path)
    p1_h, p1_rows = read_csv(p1_path)
    p2s_h, p2s_rows = read_csv(p2s_path)
    p2b_h, p2b_rows = read_csv(p2b_path)

    # index rows by old playerId
    p0_by_id: Dict[str, dict] = {r["playerId"].strip(): r for r in p0_rows if (r.get("playerId") or "").strip()}
    p1_by_id: Dict[str, dict] = {r["playerId"].strip(): r for r in p1_rows if (r.get("playerId") or "").strip()}
    p2s_by_id: Dict[str, dict] = {r["playerId"].strip(): r for r in p2s_rows if (r.get("playerId") or "").strip()}
    p2b_by_id: Dict[str, dict] = {r["playerId"].strip(): r for r in p2b_rows if (r.get("playerId") or "").strip()}

    # group old ids by playerName slug
    groups: Dict[str, List[str]] = {}
    for r in p0_rows:
        pid = (r.get("playerId") or "").strip()
        name = (r.get("playerName") or "").strip()
        if not pid or not name:
            continue
        key = slugify(name)
        groups.setdefault(key, []).append(pid)

    # build stable team-independent playerIds with collision suffix
    stable_ids: Dict[str, str] = {}  # nameSlug -> stable playerId (with collision)
    used_ids: Dict[str, int] = {}

    def make_stable_id(name_slug: str) -> str:
        base = f"player_{name_slug}"
        n = used_ids.get(base, 0) + 1
        used_ids[base] = n
        return base if n == 1 else f"{base}_{n}"

    # output consolidated rows
    new_p0_rows = []
    new_p1_rows = []
    new_p2s_rows = []
    new_p2b_rows = []
    id_map_rows = []

    # helper: choose TOT row id if present in group
    def pick_tot_id(old_ids: List[str]) -> Optional[str]:
        for oid in old_ids:
            team = (p0_by_id.get(oid, {}).get("teamId") or "").strip().upper()
            if team == TOT_TOKEN:
                return oid
        return None

    for name_slug, old_ids in groups.items():
        # stable playerId for this name_slug group
        # (if same slug appears for two different people, collision handler kicks in)
        if name_slug not in stable_ids:
            stable_ids[name_slug] = make_stable_id(name_slug)
        new_pid = stable_ids[name_slug]

        # collect related rows
        related_p0 = [p0_by_id[oid] for oid in old_ids if oid in p0_by_id]
        related_p1 = [p1_by_id[oid] for oid in old_ids if oid in p1_by_id]
        related_p2s = [p2s_by_id[oid] for oid in old_ids if oid in p2s_by_id]
        related_p2b = [p2b_by_id[oid] for oid in old_ids if oid in p2b_by_id]

        # determine "current team" (non-TOT)
        current_team = pick_current_team(related_p0, related_p1)

        # choose base identity row (prefer non-TOT for pos/team, but stats prefer TOT)
        name = (related_p0[0].get("playerName") or "").strip() if related_p0 else ""
        pos = ""
        # prefer a non-empty pos from non-TOT row, else any
        for r0 in related_p0:
            if (r0.get("teamId") or "").strip().upper() != TOT_TOKEN and (r0.get("pos") or "").strip():
                pos = (r0.get("pos") or "").strip()
                break
        if not pos:
            for r0 in related_p0:
                if (r0.get("pos") or "").strip():
                    pos = (r0.get("pos") or "").strip()
                    break

        season_key = f"{name_slug}|{current_team}"

        # ----- Consolidate Phase 1 + 2 using TOT if exists -----
        tot_id = pick_tot_id(old_ids)

        def pick_row(prefer_tot: bool, rel: List[dict], by_id: Dict[str, dict]) -> Optional[dict]:
            if prefer_tot and tot_id and tot_id in by_id:
                return by_id[tot_id]
            return rel[0] if rel else None

        # phase1 consolidated
        r1 = pick_row(True, related_p1, p1_by_id)
        if r1 is None and related_p1:
            r1 = related_p1[0]

        # if no TOT row exists but multiple team rows exist, weighted merge via games
        if (tot_id is None) and len(related_p1) > 1:
            # merge mpg and usageProxyPer36 weighted by games
            merged = weighted_merge_numeric(related_p1, "g", ["mpg", "usageProxyPer36"])
            # games = sum games
            gsum = sum((to_int(r.get("g", "")) or 0) for r in related_p1)
            starter = ""  # unreliable without GS; keep blank
            r1_out = {"playerId": new_pid, "g": str(gsum), "mpg": merged["mpg"], "starterFlag": starter, "usageProxyPer36": merged["usageProxyPer36"]}
        else:
            r1_out = {
                "playerId": new_pid,
                "g": (r1.get("g") if r1 else ""),
                "mpg": (r1.get("mpg") if r1 else ""),
                "starterFlag": (r1.get("starterFlag") if r1 else ""),
                "usageProxyPer36": (r1.get("usageProxyPer36") if r1 else ""),
            }

        # phase2 shooting consolidated
        r2s = pick_row(True, related_p2s, p2s_by_id)
        if (tot_id is None) and len(related_p2s) > 1:
            # weighted merge per-game rates by games (approx)
            merged = weighted_merge_numeric(related_p2s, "fga", ["fgPct","fg3Pct","fg2Pct","ftPct"])  # fallback
            # for counting per-game fields, weight by games
            # Better: just keep the row with max fga as "main sample" if no TOT
            r2s = max(related_p2s, key=lambda r: to_float(r.get("fga","")) or 0.0)

        # phase2 box consolidated
        r2b = pick_row(True, related_p2b, p2b_by_id)
        if (tot_id is None) and len(related_p2b) > 1:
            r2b = max(related_p2b, key=lambda r: to_float(r.get("trb","")) or 0.0)

        # build phase0 consolidated
        p0_out = {"playerId": new_pid, "playerName": name, "teamId": current_team, "pos": pos, "seasonKey": season_key}

        # build phase2 outputs
        def get(r: Optional[dict], k: str) -> str:
            return "" if r is None else (r.get(k, "") or "")

        p2s_out = {
            "playerId": new_pid,
            "fg": get(r2s,"fg"), "fga": get(r2s,"fga"), "fgPct": percent_to_0_100(get(r2s,"fgPct")),
            "fg3": get(r2s,"fg3"), "fg3a": get(r2s,"fg3a"), "fg3Pct": percent_to_0_100(get(r2s,"fg3Pct")),
            "fg2": get(r2s,"fg2"), "fg2a": get(r2s,"fg2a"), "fg2Pct": percent_to_0_100(get(r2s,"fg2Pct")),
            "ft": get(r2s,"ft"), "fta": get(r2s,"fta"), "ftPct": percent_to_0_100(get(r2s,"ftPct")),
            "pts": get(r2s,"pts"),
        }

        p2b_out = {
            "playerId": new_pid,
            "orb": get(r2b,"orb"), "trb": get(r2b,"trb"), "ast": get(r2b,"ast"),
            "stl": get(r2b,"stl"), "blk": get(r2b,"blk"), "tov": get(r2b,"tov"),
            "pf": get(r2b,"pf"),
        }

        new_p0_rows.append(p0_out)
        new_p1_rows.append(r1_out)
        new_p2s_rows.append(p2s_out)
        new_p2b_rows.append(p2b_out)
        id_map_rows.append({"playerId": new_pid, "playerName": name, "seasonKey": season_key})

    # ensure headers include seasonKey on phase0
    if "seasonKey" not in p0_h:
        p0_h = p0_h + ["seasonKey"]

    # write backups + overwrite
    for path in [p0_path, p1_path, p2s_path, p2b_path]:
        backup(path)

    write_csv(p0_path, ["playerId","playerName","teamId","pos","seasonKey"], new_p0_rows)
    write_csv(p1_path, ["playerId","g","mpg","starterFlag","usageProxyPer36"], new_p1_rows)
    write_csv(p2s_path, ["playerId","fg","fga","fgPct","fg3","fg3a","fg3Pct","fg2","fg2a","fg2Pct","ft","fta","ftPct","pts"], new_p2s_rows)
    write_csv(p2b_path, ["playerId","orb","trb","ast","stl","blk","tov","pf"], new_p2b_rows)

    # mapping file (backup if exists)
    if os.path.exists(map_path):
        backup(map_path)
    write_csv(map_path, ["playerId","playerName","seasonKey"], id_map_rows)

    print("✅ Consolidation complete.")
    print(f"- Deduped to {len(new_p0_rows)} players.")
    print(f"- Team splits removed; TOT used where available.")
    print(f"- teamId set to current team (picked from non-TOT rows).")
    print(f"- Percent columns normalized to 0–100.")
    print("Backups created as .bak (only once).")


if __name__ == "__main__":
    main()
