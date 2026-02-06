#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# -----------------------------
# helpers
# -----------------------------
def norm_name(s: str) -> str:
    """Lowercase, strip accents, remove punctuation, collapse spaces."""
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=0.8,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    s.mount("http://", HTTPAdapter(max_retries=retry))
    s.mount("https://", HTTPAdapter(max_retries=retry))
    return s


def load_teams_json(teams_json_path: Path) -> pd.DataFrame:
    """
    Accepts common Sportradar shapes; expects team UUID in 'id' and alias in 'alias'
    """
    data = json.loads(teams_json_path.read_text(encoding="utf-8"))
    teams = None

    if isinstance(data, dict):
        if isinstance(data.get("teams"), list):
            teams = data["teams"]
        elif isinstance(data.get("league", {}).get("teams"), list):
            teams = data["league"]["teams"]

    if teams is None:
        # flatten conferences/divisions if present
        teams = []
        confs = data.get("conferences") if isinstance(data, dict) else None
        if isinstance(confs, list):
            for conf in confs:
                for div in conf.get("divisions", []) or []:
                    for t in div.get("teams", []) or []:
                        teams.append(t)

    df = pd.DataFrame(teams or [])
    if df.empty:
        raise RuntimeError("teams.json did not contain a usable teams list.")

    if "id" not in df.columns or "alias" not in df.columns:
        raise RuntimeError(f"teams.json missing expected columns. Found: {df.columns.tolist()}")

    df = df.rename(columns={"id": "sr_team_uuid", "alias": "teamId"})
    df["sr_team_uuid"] = df["sr_team_uuid"].astype(str)
    df["teamId"] = df["teamId"].astype(str)  # canonical teamId in your project (IND, NYL, etc.)
    return df[["sr_team_uuid", "teamId"]].drop_duplicates()


def fetch_team_profile(session: requests.Session, base_url: str, locale: str, team_uuid: str, api_key: str) -> Dict[str, Any]:
    """
    GET {base}/{locale}/teams/{team_id}/profile.json
    We'll send both header and query param for api key to be safe.
    """
    url = f"{base_url.rstrip('/')}/{locale}/teams/{team_uuid}/profile.json"
    headers = {"x-api-key": api_key}
    params = {"api_key": api_key}
    r = session.get(url, headers=headers, params=params, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code} for team {team_uuid}: {r.text[:220]}")
    return r.json()


def extract_roster_players(profile: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Try common fields: profile['players'] or profile['team']['players'] etc.
    """
    for path in [
        ("players",),
        ("team", "players"),
        ("team", "roster"),
        ("roster",),
    ]:
        cur = profile
        ok = True
        for k in path:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                ok = False
                break
        if ok and isinstance(cur, list):
            return [p for p in cur if isinstance(p, dict)]
    return []


def load_phase0(phase0_csv: Path) -> pd.DataFrame:
    p0 = pd.read_csv(phase0_csv, low_memory=False)

    # detect name column
    if "playerName" in p0.columns:
        name_col = "playerName"
    else:
        cand = [c for c in p0.columns if "name" in c.lower() and "player" in c.lower()]
        if not cand:
            raise RuntimeError(f"phase0 has no obvious player name column. Columns: {p0.columns.tolist()}")
        name_col = cand[0]

    required = {"playerId", "teamId"}
    missing = required - set(p0.columns)
    if missing:
        raise RuntimeError(f"phase0 missing required columns: {missing}. Columns: {p0.columns.tolist()}")

    p0 = p0.dropna(subset=["playerId", name_col]).copy()
    p0["playerId"] = p0["playerId"].astype(str)
    p0["teamId"] = p0["teamId"].astype(str)
    p0["playerName"] = p0[name_col].astype(str)
    p0["name_norm"] = p0["playerName"].map(norm_name)

    # IMPORTANT:
    # don't drop duplicates here — we’ll disambiguate by team when matching
    return p0[["playerId", "teamId", "playerName", "name_norm"]]


# -----------------------------
# main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Build mapping: Sportradar player UUID -> canonical phase0 playerId using Team Profile rosters.")
    ap.add_argument("--teams-json", required=True, help="Path to Sportradar teams.json (contains team UUIDs)")
    ap.add_argument("--phase0", required=True, help="Path to phase0_players_index_2025.csv (canonical playerId + names)")
    ap.add_argument("--out", default="raw_data/maps/sr_uuid_to_phase0_playerid_2025.csv", help="Output CSV mapping path")
    ap.add_argument("--out-json", default="raw_data/maps/sr_uuid_to_phase0_playerid_2025.json", help="Output JSON mapping path")
    ap.add_argument("--issues", default="raw_data/maps/sr_uuid_to_phase0_playerid_2025_issues.csv", help="Unmatched/ambiguous report")
    ap.add_argument("--base-url", default="http://api.sportradar.com/wnba/trial/v8")
    ap.add_argument("--locale", default="en")
    ap.add_argument("--sleep", type=float, default=0.6)
    ap.add_argument("--api-key-env", default="SPORTRADAR_API_KEY")
    args = ap.parse_args()

    api_key = os.getenv(args.api_key_env)
    if not api_key:
        raise SystemExit(f"Missing API key. Set ${args.api_key_env} (PowerShell: $env:{args.api_key_env}='KEY').")

    teams_df = load_teams_json(Path(args.teams_json))
    phase0_df = load_phase0(Path(args.phase0))

    sess = make_session()
    roster_rows = []
    issues_rows = []

    for _, t in teams_df.iterrows():
        team_uuid = t["sr_team_uuid"]
        teamId = t["teamId"]  # canonical teamId (alias)

        try:
            prof = fetch_team_profile(sess, args.base_url, args.locale, team_uuid, api_key)
            players = extract_roster_players(prof)

            if not players:
                issues_rows.append({"type": "no_roster_found", "teamId": teamId, "sr_team_uuid": team_uuid})
                continue

            for p in players:
                sr_player_uuid = p.get("id") or p.get("player", {}).get("id")
                full_name = p.get("full_name") or p.get("name") or p.get("player", {}).get("full_name")

                if not sr_player_uuid or not full_name:
                    continue

                roster_rows.append({
                    "sr_player_uuid": str(sr_player_uuid),
                    "sr_player_name": str(full_name),
                    "name_norm": norm_name(str(full_name)),
                    "teamId": str(teamId),         # canonical team
                    "sr_team_uuid": str(team_uuid) # sr team uuid
                })

            time.sleep(args.sleep)

        except Exception as ex:
            issues_rows.append({
                "type": "team_fetch_error",
                "teamId": teamId,
                "sr_team_uuid": team_uuid,
                "error": str(ex),
            })

    roster_df = pd.DataFrame(roster_rows)
    if roster_df.empty:
        raise SystemExit("No roster players extracted. Check endpoint/base url/api key.")

    # -----------------------------
    # Match name -> phase0 playerId
    # -----------------------------
    # Join on normalized name (can yield multiple hits if duplicated names)
    m = roster_df.merge(phase0_df, how="left", on="name_norm", suffixes=("_sr", "_p0"))

    # Disambiguation:
    # if multiple phase0 rows share the same name_norm, prefer the one with matching teamId
    m["team_match"] = (m["teamId_sr"].astype(str) == m["teamId_p0"].astype(str)).astype(int)
    m["has_playerId"] = m["playerId"].notna().astype(int)

    # pick best per sr_player_uuid
    m = m.sort_values(["sr_player_uuid", "team_match", "has_playerId"], ascending=[True, False, False])
    best = m.groupby("sr_player_uuid", as_index=False).head(1).copy()

    # build outputs
    out_cols = [
        "sr_player_uuid",
        "sr_player_name",
        "teamId_sr",
        "sr_team_uuid",
        "playerId",
        "teamId_p0",
        "playerName",
    ]
    for c in out_cols:
        if c not in best.columns:
            best[c] = pd.NA

    best = best.rename(columns={
        "teamId_sr": "teamId",
        "teamId_p0": "phase0_teamId",
        "playerName": "phase0_playerName",
        "playerId": "canonical_playerId",
    })

    # issues report
    # 1) unmatched canonical_playerId
    miss = best["canonical_playerId"].isna()
    if miss.any():
        tmp = best.loc[miss, ["sr_player_uuid", "sr_player_name", "teamId", "sr_team_uuid"]].copy()
        tmp["type"] = "no_phase0_match"
        issues_rows.extend(tmp.to_dict("records"))

    # 2) ambiguous name matches (more than 1 phase0 row for a roster name)
    amb = m.groupby("sr_player_uuid")["playerId"].nunique(dropna=True).reset_index(name="phase0_candidate_count")
    amb = amb[amb["phase0_candidate_count"] > 1]
    if not amb.empty:
        for uuid in amb["sr_player_uuid"].tolist():
            issues_rows.append({"type": "ambiguous_name_multiple_phase0_candidates", "sr_player_uuid": uuid})

    # write mapping csv
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    best.to_csv(out_path, index=False)

    # write mapping json: sr_uuid -> canonical_playerId
    out_json_path = Path(args.out_json)
    out_json_path.parent.mkdir(parents=True, exist_ok=True)
    mapping = {}
    for _, r in best.iterrows():
        mapping[str(r["sr_player_uuid"])] = {
            "canonical_playerId": None if pd.isna(r["canonical_playerId"]) else str(r["canonical_playerId"]),
            "teamId": str(r["teamId"]),
            "sr_player_name": str(r["sr_player_name"]),
            "phase0_playerName": None if pd.isna(r.get("phase0_playerName")) else str(r.get("phase0_playerName")),
            "phase0_teamId": None if pd.isna(r.get("phase0_teamId")) else str(r.get("phase0_teamId")),
        }
    out_json_path.write_text(json.dumps(mapping, indent=2), encoding="utf-8")

    # issues csv
    issues_path = Path(args.issues)
    issues_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(issues_rows).to_csv(issues_path, index=False)

    print("wrote:")
    print(f" - {out_path}")
    print(f" - {out_json_path}")
    print(f" - {issues_path}")
    print(f"mapped {best['canonical_playerId'].notna().sum()}/{len(best)} roster players to phase0")


if __name__ == "__main__":
    main()
