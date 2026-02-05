#!/usr/bin/env python3
"""
Cross-reference phase0 teamId using an Excel totals file (wnba-player-stats.xlsx).

Inputs:
- raw_data/phase0_players_index_2025.csv
- raw_data/wnba-player-stats.xlsx  (must contain columns like Player + Team somewhere)

Outputs:
- raw_data/team_verify_from_xlsx_2025.csv
- Prints dialogue lines for every player needing a change.

Run:
  python tools/verify_teams_from_xlsx.py --year 2025
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import unicodedata
from difflib import SequenceMatcher
from typing import Dict, List, Optional, Tuple

import pandas as pd


RAW_DIR = "raw_data"

TEAM_ALIAS = {
    # If your xlsx uses full names or different abbreviations, map them here.
    # Example: "LAS" etc. (leave empty unless needed)
}

SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}


def normalize_name(name: str) -> str:
    name = (name or "").strip()
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower()
    name = re.sub(r"[^a-z0-9\s-]", " ", name)
    name = name.replace("-", " ")
    parts = [p for p in name.split() if p]
    if parts and parts[-1] in SUFFIXES:
        parts = parts[:-1]
    return " ".join(parts)


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def best_fuzzy_match(query: str, candidates: List[str], min_score: float) -> Tuple[Optional[str], float]:
    best = None
    best_sc = 0.0
    for c in candidates:
        sc = similarity(query, c)
        if sc > best_sc:
            best_sc = sc
            best = c
    if best is not None and best_sc >= min_score:
        return best, best_sc
    return None, best_sc


def coerce_team(val: str) -> str:
    t = (val or "").strip().upper()
    if t in TEAM_ALIAS:
        return TEAM_ALIAS[t]
    return t


def find_player_team_table_in_xlsx(xlsx_path: str) -> pd.DataFrame:
    """
    Loads the first sheet that contains a table with 'Player' and 'Team'
    columns somewhere (even if the header starts on row 2/3).
    """
    xl = pd.ExcelFile(xlsx_path)

    for sheet in xl.sheet_names:
        # Read a chunk without headers to search for the header row
        preview = xl.parse(sheet_name=sheet, header=None, nrows=20)
        # find a row containing both "Player" and "Team"
        header_row_idx = None
        for i in range(len(preview)):
            row = preview.iloc[i].astype(str).str.strip().str.lower().tolist()
            if "player" in row and "team" in row:
                header_row_idx = i
                break

        if header_row_idx is None:
            continue

        df = xl.parse(sheet_name=sheet, header=header_row_idx)
        # normalize column names
        cols = {c: str(c).strip() for c in df.columns}
        df.rename(columns=cols, inplace=True)

        if "Player" in df.columns and "Team" in df.columns:
            # drop empty player rows
            df = df[df["Player"].notna()]
            df["Player"] = df["Player"].astype(str).str.strip()
            df["Team"] = df["Team"].astype(str).str.strip()
            return df

    raise RuntimeError("Could not find a sheet/table containing 'Player' and 'Team' columns in the xlsx.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--year", type=int, default=2025)
    ap.add_argument("--fuzzy", type=float, default=0.92, help="Fuzzy match threshold (0-1)")
    ap.add_argument("--phase0", type=str, default="", help="Override phase0 path")
    ap.add_argument("--xlsx", type=str, default="", help="Override xlsx path")
    args = ap.parse_args()

    phase0_path = args.phase0 or os.path.join(RAW_DIR, f"phase0_players_index_{args.year}.csv")
    xlsx_path = args.xlsx or os.path.join(RAW_DIR, "wnba-player-stats.xlsx")

    if not os.path.exists(phase0_path):
        raise FileNotFoundError(f"Missing phase0: {phase0_path}")
    if not os.path.exists(xlsx_path):
        raise FileNotFoundError(f"Missing xlsx: {xlsx_path}")

    p0 = pd.read_csv(phase0_path)
    for col in ["playerId", "playerName", "teamId"]:
        if col not in p0.columns:
            raise RuntimeError(f"phase0 missing required column: {col}")

    p0["norm"] = p0["playerName"].astype(str).apply(normalize_name)
    p0["teamId"] = p0["teamId"].astype(str).apply(coerce_team)

    ref = find_player_team_table_in_xlsx(xlsx_path)
    ref["norm"] = ref["Player"].astype(str).apply(normalize_name)
    ref["Team"] = ref["Team"].astype(str).apply(coerce_team)

    # Exact map
    team_by_norm: Dict[str, str] = dict(zip(ref["norm"], ref["Team"]))
    name_by_norm: Dict[str, str] = dict(zip(ref["norm"], ref["Player"]))
    norms_list = ref["norm"].tolist()

    out_rows = []
    dialogues = []

    verified = mismatches = not_found = 0

    for _, r in p0.iterrows():
        pid = str(r["playerId"]).strip()
        name = str(r["playerName"]).strip()
        team_phase0 = str(r["teamId"]).strip().upper()
        n = str(r["norm"])

        ref_team = team_by_norm.get(n)
        ref_name = name_by_norm.get(n)
        match_type = "exact"
        match_score = 1.0

        if ref_team is None:
            best, sc = best_fuzzy_match(n, norms_list, args.fuzzy)
            if best is not None:
                ref_team = team_by_norm.get(best)
                ref_name = name_by_norm.get(best)
                match_type = "fuzzy"
                match_score = sc
            else:
                match_type = "none"
                match_score = 0.0

        action = "OK"
        dialogue = ""

        if ref_team is None:
            not_found += 1
            action = "NOT_FOUND_IN_XLSX"
            dialogue = f"Could not verify {name} ({team_phase0}) because no match was found in the xlsx."
        else:
            verified += 1
            ref_team_u = str(ref_team).strip().upper()
            if ref_team_u != team_phase0:
                mismatches += 1
                action = "CHANGE_TEAM"
                dialogue = (
                    f"Change {name}: teamId {team_phase0} -> {ref_team_u} "
                    f"(matched '{ref_name}' via {match_type}, score={match_score:.3f})."
                )
                dialogues.append(dialogue)

        out_rows.append({
            "playerId": pid,
            "playerName": name,
            "phase0_teamId": team_phase0,
            "xlsx_teamId": "" if ref_team is None else str(ref_team).strip().upper(),
            "matchType": match_type,
            "matchScore": "" if ref_team is None else f"{match_score:.3f}",
            "action": action,
            "dialogue": dialogue,
        })

    out_path = os.path.join(RAW_DIR, f"team_verify_from_xlsx_{args.year}.csv")
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    print(f"✅ Wrote: {out_path}")
    print(f"Verified (matched): {verified}")
    print(f"Mismatches: {mismatches}")
    print(f"Not found: {not_found}")
    if mismatches:
        print("\n--- DIALOGUE FOR CHANGES (copy/paste) ---")
        for line in dialogues:
            print(line)


if __name__ == "__main__":
    main()
