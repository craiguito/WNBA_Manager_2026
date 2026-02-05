#!/usr/bin/env python3
"""
Parse PRE.txt that is fixed-width (space-aligned) into phase CSVs.
- Uses pandas.read_fwf (fixed width) so it works even when columns are merged like "Team Pos".
- Skips repeated headers inside the file.
- Prefers TOT rows if a player has multiple team stints.
- Attaches playerId by joining against phase0 index or players_id_map.

Inputs:
  raw_data/PRE.txt
  raw_data/phase0_players_index_2025.csv (preferred) OR raw_data/players_id_map.csv

Outputs:
  raw_data/phase4_misc_impact_2025_stints.csv
  raw_data/phase4_misc_impact_2025_players.csv
  raw_data/phase3_playmaking_extras_2025.csv
  raw_data/phase4_discipline_impact_2025.csv
"""

import os
import re
import unicodedata
from typing import Dict, List, Tuple, Optional

import pandas as pd


IN_PRE = "raw_data/PRE.txt"
PHASE0_INDEX = "raw_data/phase0_players_index_2025.csv"
ID_MAP = "raw_data/players_id_map.csv"

OUT_STINTS = "raw_data/phase4_misc_impact_2025_stints.csv"
OUT_PLAYERS = "raw_data/phase4_misc_impact_2025_players.csv"
OUT_P3 = "raw_data/phase3_playmaking_extras_2025.csv"
OUT_P4 = "raw_data/phase4_discipline_impact_2025.csv"


def normalize_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _find_header_row(df: pd.DataFrame) -> int:
    """
    Find the row index that contains the header names.
    In fixed-width exports, the header row usually contains 'Player' and 'Team'/'Tm' and 'Pos'.
    """
    for i in range(min(len(df), 200)):  # scan first 200 lines
        row = df.iloc[i].astype(str).tolist()
        joined = " ".join(row).strip()
        if not joined:
            continue
        if "Player" in joined and ("Team" in joined or "Tm" in joined) and ("Pos" in joined):
            return i
    raise RuntimeError("Could not locate header row containing Player/Team/Pos in PRE.txt")


def _clean_header(cols: List[str]) -> List[str]:
    cols2 = []
    for c in cols:
        c = str(c).strip()
        c = re.sub(r"\s+", " ", c)
        cols2.append(c)
    return cols2


def parse_pre_fixed_width(path: str) -> pd.DataFrame:
    """
    Read file as fixed-width. Handle repeated headers.
    """
    # read raw fixed-width; let pandas infer columns
    raw = pd.read_fwf(path, header=None, colspecs="infer")

    hdr_i = _find_header_row(raw)
    header = _clean_header(raw.iloc[hdr_i].astype(str).tolist())

    df = raw.iloc[hdr_i + 1 :].copy()
    df.columns = header

    # drop empty rows
    df = df.dropna(how="all")

    # remove repeated header rows inside the data
    def is_header_like(row) -> bool:
        joined = " ".join([str(x) for x in row.tolist()]).strip()
        return ("Player" in joined and ("Team" in joined or "Tm" in joined) and ("Pos" in joined))

    mask = df.apply(is_header_like, axis=1)
    df = df.loc[~mask].copy()

    # normalize column names a bit (some exports combine "+/- Per 100 Poss." etc)
    # We'll map many variants to canonical keys.
    col_map = {}

    def canon(c: str) -> str:
        c = unicodedata.normalize("NFKD", c)
        c = "".join(ch for ch in c if not unicodedata.combining(ch))
        c = c.lower().strip()
        c = re.sub(r"\s+", "", c)
        c = c.replace("–", "-")
        c = re.sub(r"[^a-z0-9\+\-\/\.]", "", c)
        return c

    # Build lookup by canonicalized name
    cols_by_canon: Dict[str, str] = {canon(c): c for c in df.columns}

    def map_one(target: str, candidates: List[str]):
        for cand in candidates:
            if cand in cols_by_canon:
                col_map[cols_by_canon[cand]] = target
                return

    # required
    map_one("playerName", ["player"])
    map_one("teamId", ["team", "tm"])
    map_one("pos", ["pos"])
    map_one("games", ["g"])
    map_one("minutes", ["mp"])

    # impact
    map_one("pmOnCourtPer100", ["oncourt", "+/-per100poss.", "+/-per100poss"])
    map_one("pmOnOffPer100", ["on-off", "onoff", "+/-per100poss..1", "+/-per100poss.1"])

    # turnovers
    map_one("tovBadPass", ["badpass"])
    map_one("tovLostBall", ["lostball"])

    # fouls (two shoot/off pairs: committed & drawn)
    # In many exports these are just 'Shoot' and 'Off.' twice; read_fwf often makes them distinct by spacing.
    # We'll detect by partial matches.
    shoot_cols = [c for c in df.columns if "Shoot" in str(c)]
    off_cols = [c for c in df.columns if "Off" in str(c) or "Off." in str(c)]

    # try to assign first shoot/off to committed, second to drawn
    if len(shoot_cols) >= 1:
        col_map[shoot_cols[0]] = "foulsCommittedShooting"
    if len(off_cols) >= 1:
        col_map[off_cols[0]] = "foulsCommittedOffensive"
    if len(shoot_cols) >= 2:
        col_map[shoot_cols[1]] = "foulsDrawnShooting"
    if len(off_cols) >= 2:
        col_map[off_cols[1]] = "foulsDrawnOffensive"

    # misc
    map_one("pga", ["pga"])
    map_one("and1", ["and1"])
    map_one("shotsBlockedOnAttempt", ["blkd"])

    # apply rename
    df = df.rename(columns=col_map)

    # hard check for teamId
    if "teamId" not in df.columns:
        raise RuntimeError(f"Still could not map teamId. Columns seen: {list(df.columns)[:40]}")

    # clean strings
    for c in ["playerName", "teamId", "pos"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    # normalize for joining
    df["nameKey"] = df["playerName"].apply(normalize_name)
    df["teamId"] = df["teamId"].astype(str).str.strip()

    # numeric coercion
    num_cols = [
        "games", "minutes",
        "pmOnCourtPer100", "pmOnOffPer100",
        "tovBadPass", "tovLostBall",
        "foulsCommittedShooting", "foulsCommittedOffensive",
        "foulsDrawnShooting", "foulsDrawnOffensive",
        "pga", "and1", "shotsBlockedOnAttempt",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def attach_player_ids(df: pd.DataFrame) -> pd.DataFrame:
    map_df = None
    if os.path.exists(PHASE0_INDEX):
        map_df = pd.read_csv(PHASE0_INDEX)
        if "playerId" not in map_df.columns or "playerName" not in map_df.columns:
            map_df = None
    if map_df is None and os.path.exists(ID_MAP):
        map_df = pd.read_csv(ID_MAP)
        if "playerId" not in map_df.columns or "playerName" not in map_df.columns:
            map_df = None

    if map_df is None:
        df["playerId"] = ""
        print("⚠️ phase0 index / id_map not found. playerId left blank.")
        return df

    map_df["nameKey"] = map_df["playerName"].apply(normalize_name)
    map_df["teamId"] = map_df.get("teamId", "").astype(str).str.strip()

    buckets: Dict[str, List[Tuple[str, str]]] = {}
    for _, r in map_df.iterrows():
        nk = r["nameKey"]
        tid = str(r.get("teamId", "")).strip()
        pid = str(r["playerId"]).strip()
        buckets.setdefault(nk, []).append((tid, pid))

    def resolve(row) -> str:
        nk = row["nameKey"]
        tid = str(row.get("teamId", "")).strip()
        cands = buckets.get(nk, [])
        if not cands:
            return ""
        # team match if possible
        for ctid, pid in cands:
            if ctid and ctid == tid:
                return pid
        return cands[0][1]

    df["playerId"] = df.apply(resolve, axis=1)
    return df


def tot_preferred(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for _, g in df.groupby("nameKey", dropna=False):
        g = g.copy()
        team_upper = g["teamId"].astype(str).str.strip().str.upper()
        tot = g[team_upper == "TOT"]
        if len(tot) > 0:
            pick = tot.sort_values(["minutes", "games"], ascending=False).iloc[0]
        else:
            pick = g.sort_values(["minutes", "games"], ascending=False).iloc[0]
        out.append(pick)
    return pd.DataFrame(out)


def main():
    os.makedirs("raw_data", exist_ok=True)
    if not os.path.exists(IN_PRE):
        raise FileNotFoundError(f"Missing {IN_PRE}. Put your PRE file there.")

    df = parse_pre_fixed_width(IN_PRE)
    df = attach_player_ids(df)

    keep_cols = [
        "playerId", "playerName", "teamId", "pos", "games", "minutes",
        "pmOnCourtPer100", "pmOnOffPer100",
        "tovBadPass", "tovLostBall",
        "foulsCommittedShooting", "foulsCommittedOffensive",
        "foulsDrawnShooting", "foulsDrawnOffensive",
        "pga", "and1", "shotsBlockedOnAttempt",
    ]
    keep_cols = [c for c in keep_cols if c in df.columns]

    stints = df[keep_cols].copy()
    stints.to_csv(OUT_STINTS, index=False)

    players = tot_preferred(df)[keep_cols].copy()
    players.to_csv(OUT_PLAYERS, index=False)

    # phase 3 extras
    p3_cols = [c for c in ["playerId","playerName","teamId","pos","tovBadPass","tovLostBall","pga"] if c in players.columns]
    players[p3_cols].to_csv(OUT_P3, index=False)

    # phase 4 impact + discipline
    p4_cols = [c for c in [
        "playerId","playerName","teamId","pos",
        "pmOnCourtPer100","pmOnOffPer100",
        "foulsCommittedShooting","foulsCommittedOffensive",
        "foulsDrawnShooting","foulsDrawnOffensive",
        "and1","shotsBlockedOnAttempt"
    ] if c in players.columns]
    players[p4_cols].to_csv(OUT_P4, index=False)

    print(f"✅ wrote {OUT_STINTS} ({len(stints)} rows)")
    print(f"✅ wrote {OUT_PLAYERS} ({len(players)} rows, TOT-preferred)")
    print(f"✅ wrote {OUT_P3}")
    print(f"✅ wrote {OUT_P4}")

    if "playerId" in players.columns:
        missing = (players["playerId"].astype(str).str.strip() == "").sum()
        if missing:
            print(f"⚠️ {missing} players missing playerId join (name formatting/accents). Fixable.")


if __name__ == "__main__":
    main()

