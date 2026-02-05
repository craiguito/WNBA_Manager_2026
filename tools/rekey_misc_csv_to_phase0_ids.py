#!/usr/bin/env python3
"""
Re-key a misc/impact CSV to Phase 0 canonical ids.

What it does:
- Reads Phase 0 index CSV (canonical): raw_data/phase0_players_index_2025.csv
  expected columns: playerId, playerName, teamId (pos optional)
- Reads an input CSV (your misc/impact output) with columns including:
  playerId, playerName, teamId, ... (anything else preserved)
- Replaces playerId with Phase 0 playerId by matching normalized playerName.
- Replaces teamId == 'TOT' with the Phase 0 teamId for that playerName.
- Keeps original ids in oldPlayerId/oldTeamId (optional flag)

Usage:
  python tools/rekey_misc_csv_to_phase0_ids.py \
    --phase0 raw_data/phase0_players_index_2025.csv \
    --in data/phase2_impact_misc_2025.csv \
    --out data/phase2_impact_misc_2025_rekeyed.csv
"""

import argparse
import re
import unicodedata
import pandas as pd


def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    # keep letters/numbers/spaces/apostrophes/hyphens
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase0", required=True, help="Phase 0 canonical index CSV (playerId, playerName, teamId)")
    ap.add_argument("--in", dest="inp", required=True, help="Input misc/impact CSV to re-key")
    ap.add_argument("--out", required=True, help="Output CSV")
    ap.add_argument("--keep_old", action="store_true", help="Keep oldPlayerId/oldTeamId columns")
    args = ap.parse_args()

    p0 = pd.read_csv(args.phase0)
    df = pd.read_csv(args.inp)

    # basic column checks
    for col in ["playerName", "playerId", "teamId"]:
        if col not in df.columns:
            raise RuntimeError(f"Input CSV missing required column: {col}")
    for col in ["playerName", "playerId", "teamId"]:
        if col not in p0.columns:
            raise RuntimeError(f"Phase0 CSV missing required column: {col}")

    # normalize join keys
    p0 = p0.copy()
    df = df.copy()
    p0["nameKey"] = p0["playerName"].apply(normalize_name)
    df["nameKey"] = df["playerName"].apply(normalize_name)

    # build mapping: nameKey -> (playerId, teamId)
    # if duplicates exist in phase0, prefer the row with non-empty teamId; otherwise first.
    p0_sorted = p0.copy()
    p0_sorted["teamId"] = p0_sorted["teamId"].astype(str).str.strip()
    p0_sorted["playerId"] = p0_sorted["playerId"].astype(str).str.strip()
    p0_sorted = p0_sorted.sort_values(by=["nameKey", "teamId"], ascending=[True, False])

    # collapse to first per nameKey
    p0_one = p0_sorted.drop_duplicates(subset=["nameKey"], keep="first")

    id_map = dict(zip(p0_one["nameKey"], p0_one["playerId"]))
    team_map = dict(zip(p0_one["nameKey"], p0_one["teamId"]))

    # optionally keep old columns
    if args.keep_old:
        df["oldPlayerId"] = df["playerId"]
        df["oldTeamId"] = df["teamId"]

    # replace playerId where we can
    df["playerId"] = df["nameKey"].map(id_map).fillna(df["playerId"])

    # replace TOT with phase0 teamId (and also clean team casing/spaces)
    df["teamId"] = df["teamId"].astype(str).str.strip()
    is_tot = df["teamId"].str.upper() == "TOT"

    # only replace TOT if we have a known team for that player
    df.loc[is_tot, "teamId"] = df.loc[is_tot, "nameKey"].map(team_map).fillna(df.loc[is_tot, "teamId"])

    # report
    missing_id = df["nameKey"].map(id_map).isna().sum()
    still_tot = (df["teamId"].astype(str).str.upper() == "TOT").sum()

    print(f"✅ Re-keyed playerId using phase0 for {len(df) - missing_id}/{len(df)} rows")
    if missing_id:
        print(f"⚠️ {missing_id} rows could not be matched by name (left original playerId).")
        # show a few examples
        ex = df.loc[df["nameKey"].map(id_map).isna(), "playerName"].drop_duplicates().head(15).tolist()
        if ex:
            print("   examples:", ", ".join(ex))

    if still_tot:
        print(f"⚠️ {still_tot} rows still have teamId=TOT (phase0 mapping missing or ambiguous).")

    # drop helper key
    df = df.drop(columns=["nameKey"])

    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"✅ wrote {args.out}")


if __name__ == "__main__":
    main()
