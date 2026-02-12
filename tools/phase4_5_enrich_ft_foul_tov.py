#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re
import unicodedata
from difflib import get_close_matches

import pandas as pd


# -----------------------------
# normalization + aliasing
# -----------------------------
SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", re.IGNORECASE)

def strip_accents(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(ch for ch in s if not unicodedata.combining(ch))

def norm_name(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    s = strip_accents(s)
    s = s.lower()

    # normalize punctuation/spaces
    s = s.replace("’", "'")
    s = re.sub(r"[^\w\s'-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()

    # strip suffix tokens at end
    s = SUFFIX_RE.sub("", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_aliases(path: Path | None) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    if path.suffix.lower() == ".json":
        import json
        return {norm_name(k): norm_name(v) for k, v in json.loads(path.read_text(encoding="utf-8")).items()}
    # csv: raw,canon
    df = pd.read_csv(path)
    if not {"raw","canon"} <= set(df.columns):
        raise RuntimeError("Alias CSV must have columns: raw, canon")
    return {norm_name(r.raw): norm_name(r.canon) for r in df.itertuples(index=False)}

def apply_alias(n: str, alias_map: dict[str, str]) -> str:
    return alias_map.get(n, n)


# -----------------------------
# phase0 lookup
# -----------------------------
def load_phase0(phase0_csv: Path) -> pd.DataFrame:
    p0 = pd.read_csv(phase0_csv, low_memory=False)
    need = {"playerId", "playerName", "teamId"}
    missing = need - set(p0.columns)
    if missing:
        raise RuntimeError(f"phase0 missing {missing}. cols={p0.columns.tolist()}")
    p0 = p0.copy()
    p0["playerId"] = p0["playerId"].astype(str)
    p0["teamId"] = p0["teamId"].astype(str)
    p0["playerName_norm"] = p0["playerName"].map(norm_name)
    return p0

def build_name_lookup(p0: pd.DataFrame) -> dict[str, list[tuple[str,str,str]]]:
    lut: dict[str, list[tuple[str,str,str]]] = {}
    for r in p0.itertuples(index=False):
        k = r.playerName_norm
        if not k:
            continue
        lut.setdefault(k, []).append((r.playerId, r.teamId, r.playerName))
    return lut


def resolve_player_id(
    raw_name: str,
    lut: dict[str, list[tuple[str,str,str]]],
    alias_map: dict[str, str],
    preferred_team: str | None,
) -> tuple[str | None, str, str]:
    """
    returns (playerId|None, normalized_name, resolution_status)
    status: exact / team_exact / ambiguous / not_found
    """
    n = norm_name(raw_name)
    n = apply_alias(n, alias_map)

    cands = lut.get(n, [])
    if not cands:
        return None, n, "not_found"
    if len(cands) == 1:
        return cands[0][0], n, "exact"

    if preferred_team:
        for pid, tid, _ in cands:
            if tid == preferred_team:
                return pid, n, "team_exact"
    return None, n, "ambiguous"


# -----------------------------
# clutch + parsing
# -----------------------------
def parse_clock_to_sec(clock: str) -> int | None:
    if not clock or ":" not in str(clock):
        return None
    try:
        mm, ss = str(clock).split(":")
        return int(mm) * 60 + int(ss)
    except Exception:
        return None

def is_clutch(period_number, clock: str) -> bool:
    # clutch = last 2:00 of 4th or any OT
    try:
        p = int(period_number)
    except Exception:
        return False
    sec = parse_clock_to_sec(clock)
    if sec is None:
        return False
    return (p == 4 and sec <= 120) or (p >= 5)

# Free throws
RE_FT = re.compile(r"^(.+?)\s+(makes|misses)\s+.*free throw", re.IGNORECASE)

# Turnovers
RE_TOV_A = re.compile(r"^(.+?)\s+turnover\b", re.IGNORECASE)             # "NAME turnover ..."
RE_TOV_B = re.compile(r"turnover\s+by\s+(.+?)(?:\(|$)", re.IGNORECASE)    # "Turnover by NAME"
RE_TOV_TEAM = re.compile(r"^\s*([A-Za-z].+?)\s+turnover\b", re.IGNORECASE)

# Fouls
RE_FOUL_BY = re.compile(r"foul\s+by\s+(.+?)(?:\(|$)", re.IGNORECASE)
RE_FOUL_DRAWN_BY = re.compile(r"drawn\s+by\s+(.+?)(?:\(|$)", re.IGNORECASE)
RE_FOUL_LEAD = re.compile(r"^(.+?)\s+.*\bfoul\b", re.IGNORECASE)          # "NAME personal foul"
RE_FOUL_ON = re.compile(r"foul\s+on\s+(.+?)(?:\(|$)", re.IGNORECASE)       # sometimes "foul on NAME"


def main():
    ap = argparse.ArgumentParser(description="Enrich Phase 4.5 with FT/Foul/Turnover parsing from pbp_events_canonical.")
    ap.add_argument("--events", required=True, help="pbp_events_canonical.csv")
    ap.add_argument("--phase0", required=True, help="phase0_players_index_2025.csv")
    ap.add_argument("--phase45", required=True, help="existing phase4_5_player_action_context_2025.csv")
    ap.add_argument("--season", default="2025")
    ap.add_argument("--aliases", default="", help="optional alias file (json map or csv raw,canon)")
    ap.add_argument("--out-extra", default="derived/phase4_5_extra_actions_2025.csv")
    ap.add_argument("--out-merged", default="derived/phase4_5_player_action_context_2025_merged.csv")
    ap.add_argument("--out-unparsed", default="derived/phase4_5_unparsed_ft_foul_tov_samples.csv")
    ap.add_argument("--out-name-suggestions", default="derived/phase4_5_name_suggestions.csv")
    args = ap.parse_args()

    events = pd.read_csv(args.events, low_memory=False)
    p0 = load_phase0(Path(args.phase0))
    lut = build_name_lookup(p0)

    alias_path = Path(args.aliases) if args.aliases else None
    alias_map = load_aliases(alias_path)

    # columns for team context (best effort)
    team_att_col = "attribution_team_id" if "attribution_team_id" in events.columns else None

    # collect actions
    rows: list[dict] = []
    unparsed: list[dict] = []
    not_found_names: list[dict] = []

    def add(pid: str, team_id: str | None, stat: str, inc: float, game_id: str):
        rows.append({
            "season_year": args.season,
            "player_id": pid,
            "team_id": team_id or "",
            "stat": stat,
            "inc": inc,
            "game_id": game_id,
        })

    for r in events.itertuples(index=False):
        et = str(getattr(r, "event_type", "") or "").lower()
        desc = str(getattr(r, "description", "") or "")
        game_id = str(getattr(r, "game_id", ""))
        period = getattr(r, "period_number", None)
        clock = str(getattr(r, "clock", "") or "")

        preferred_team = str(getattr(r, team_att_col)) if team_att_col else None
        clutch = is_clutch(period, clock)

        # ---------- FREE THROWS ----------
        if ("freethrow" in et) or ("free_throw" in et) or ("free throw" in desc.lower()):
            m = RE_FT.search(desc)
            if not m:
                # some feeds: "NAME makes technical free throw"
                # try looser: leading name up to "makes/misses"
                m2 = re.match(r"^(.+?)\s+(makes|misses)\b", desc, flags=re.IGNORECASE)
                if not m2 or "free throw" not in desc.lower():
                    unparsed.append({"game_id": game_id, "event_type": et, "bucket": "freethrow", "description": desc})
                    continue
                shooter_name = m2.group(1).strip()
                verb = m2.group(2).lower()
            else:
                shooter_name = m.group(1).strip()
                verb = m.group(2).lower()

            pid, n, status = resolve_player_id(shooter_name, lut, alias_map, preferred_team)
            if not pid:
                not_found_names.append({"bucket":"freethrow", "raw_name": shooter_name, "norm": n, "status": status, "game_id": game_id, "desc": desc})
                continue

            add(pid, preferred_team, "fta", 1, game_id)
            if verb == "makes":
                add(pid, preferred_team, "ftm", 1, game_id)
            if clutch:
                add(pid, preferred_team, "clutch_fta", 1, game_id)
            continue

        # ---------- TURNOVERS ----------
        if ("turnover" in et) or ("turnover" in desc.lower()):
            # ignore pure team turnovers like "Mystics turnover (5-second violation)"
            # but still try: if name exists in phase0, we count; otherwise ignore
            name = None
            m = RE_TOV_A.search(desc)
            if m:
                name = m.group(1).strip()
            else:
                m = RE_TOV_B.search(desc)
                if m:
                    name = m.group(1).strip()

            if not name:
                unparsed.append({"game_id": game_id, "event_type": et, "bucket": "turnover", "description": desc})
                continue

            pid, n, status = resolve_player_id(name, lut, alias_map, preferred_team)
            if not pid:
                # if it's a team name, ignore
                if n.endswith("turnover"):
                    continue
                not_found_names.append({"bucket":"turnover", "raw_name": name, "norm": n, "status": status, "game_id": game_id, "desc": desc})
                continue

            add(pid, preferred_team, "tov", 1, game_id)

            # subtype column if present
            tov_type = getattr(r, "turnover_type", "")
            tov_type = str(tov_type or "").lower().strip()
            if tov_type:
                key = re.sub(r"[^a-z0-9]+", "_", tov_type).strip("_")
                add(pid, preferred_team, f"tov_{key}", 1, game_id)
            if clutch:
                add(pid, preferred_team, "clutch_tov", 1, game_id)
            continue

        # ---------- FOULS ----------
        if "foul" in et:
            fouler = None
            drawn = None

            m = RE_FOUL_BY.search(desc)
            if m:
                fouler = m.group(1).strip()

            m = RE_FOUL_ON.search(desc)
            if m and not drawn:
                drawn = m.group(1).strip()

            m = RE_FOUL_DRAWN_BY.search(desc)
            if m:
                drawn = m.group(1).strip()

            if not fouler:
                m = RE_FOUL_LEAD.search(desc)
                if m:
                    fouler = m.group(1).strip()

            any_parsed = False

            if fouler:
                pid, n, status = resolve_player_id(fouler, lut, alias_map, None)
                if pid:
                    add(pid, preferred_team, "pf_committed", 1, game_id)
                    if clutch:
                        add(pid, preferred_team, "clutch_pf_committed", 1, game_id)
                    any_parsed = True
                else:
                    not_found_names.append({"bucket":"foul_committed", "raw_name": fouler, "norm": n, "status": status, "game_id": game_id, "desc": desc})

            if drawn:
                pid, n, status = resolve_player_id(drawn, lut, alias_map, None)
                if pid:
                    add(pid, preferred_team, "pf_drawn", 1, game_id)
                    if clutch:
                        add(pid, preferred_team, "clutch_pf_drawn", 1, game_id)
                    any_parsed = True
                else:
                    not_found_names.append({"bucket":"foul_drawn", "raw_name": drawn, "norm": n, "status": status, "game_id": game_id, "desc": desc})

            if not any_parsed:
                unparsed.append({"game_id": game_id, "event_type": et, "bucket": "foul", "description": desc})
            continue

    # ---------- aggregate extra actions ----------
    if rows:
        df = pd.DataFrame(rows)
        agg = df.groupby(["season_year", "player_id", "stat"], as_index=False)["inc"].sum()
        wide = agg.pivot_table(
            index=["season_year", "player_id"],
            columns="stat",
            values="inc",
            aggfunc="sum",
            fill_value=0.0
        ).reset_index()

        # attach canonical teamId from phase0 (authoritative)
        wide = wide.merge(
            p0[["playerId","teamId"]].rename(columns={"playerId":"player_id","teamId":"team_id"}),
            on="player_id",
            how="left"
        )
    else:
        wide = pd.DataFrame(columns=["season_year","player_id","team_id"])

    out_extra = Path(args.out_extra)
    out_extra.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(out_extra, index=False)
    print("wrote extra:", out_extra, "rows:", len(wide))

    # ---------- build name suggestions for not_found ----------
    # use close match suggestions from phase0 normalized names
    p0_names = sorted(set(p0["playerName_norm"].tolist()))
    sug_rows = []
    for item in not_found_names:
        n = item["norm"]
        if not n:
            continue
        matches = get_close_matches(n, p0_names, n=5, cutoff=0.86)
        if matches:
            for m in matches:
                # list candidate canonical names for that normalized key
                cands = lut.get(m, [])
                for pid, tid, orig in cands[:3]:
                    sug_rows.append({
                        "bucket": item["bucket"],
                        "raw_name": item["raw_name"],
                        "norm": n,
                        "suggest_norm": m,
                        "suggest_playerName": orig,
                        "suggest_playerId": pid,
                        "suggest_teamId": tid,
                        "game_id": item.get("game_id",""),
                    })
        else:
            sug_rows.append({
                "bucket": item["bucket"],
                "raw_name": item["raw_name"],
                "norm": n,
                "suggest_norm": "",
                "suggest_playerName": "",
                "suggest_playerId": "",
                "suggest_teamId": "",
                "game_id": item.get("game_id",""),
            })

    out_sug = Path(args.out_name_suggestions)
    out_sug.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(sug_rows).to_csv(out_sug, index=False)
    print("wrote suggestions:", out_sug, "rows:", len(sug_rows))

    # ---------- write unparsed samples ----------
    out_unp = Path(args.out_unparsed)
    out_unp.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(unparsed).to_csv(out_unp, index=False)
    print("wrote unparsed:", out_unp, "rows:", len(unparsed))

    # ---------- merge into existing phase4.5 file ----------
    base = pd.read_csv(args.phase45, low_memory=False)
    # normalize keys
    base["player_id"] = base["player_id"].astype(str)
    if "season_year" not in base.columns:
        base["season_year"] = args.season
    base["season_year"] = base["season_year"].astype(str)

    wide["player_id"] = wide["player_id"].astype(str)
    wide["season_year"] = wide["season_year"].astype(str)

    merged = base.merge(
        wide.drop(columns=["team_id"], errors="ignore"),
        on=["season_year","player_id"],
        how="left",
        suffixes=("", "_new"),
    )

    # fill NaNs for new columns with 0
    new_cols = [c for c in merged.columns if c.endswith("_new")]
    for c in new_cols:
        merged[c] = merged[c].fillna(0)

    # If base already has some of these columns, sum them; else rename *_new -> original
    for c in new_cols:
        base_name = c[:-4]
        if base_name in merged.columns:
            merged[base_name] = merged[base_name].fillna(0) + merged[c]
            merged.drop(columns=[c], inplace=True)
        else:
            merged.rename(columns={c: base_name}, inplace=True)

    out_merged = Path(args.out_merged)
    out_merged.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_merged, index=False)
    print("wrote merged:", out_merged, "rows:", len(merged))


if __name__ == "__main__":
    main()
