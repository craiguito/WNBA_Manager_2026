#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re
import unicodedata
from difflib import get_close_matches
import pandas as pd


# -----------------------------
# name normalization + aliases
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
    s = s.lower().replace("’", "'")
    s = re.sub(r"[^\w\s'-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = SUFFIX_RE.sub("", s).strip()
    s = re.sub(r"\s+", " ", s).strip()
    return s

def load_aliases(path: Path | None) -> dict[str, str]:
    if not path or not path.exists():
        return {}
    if path.suffix.lower() == ".json":
        import json
        d = json.loads(path.read_text(encoding="utf-8"))
        return {norm_name(k): norm_name(v) for k, v in d.items()}
    # CSV: raw,canon
    df = pd.read_csv(path)
    if not {"raw", "canon"} <= set(df.columns):
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

def build_name_lookup(p0: pd.DataFrame) -> dict[str, list[tuple[str, str, str]]]:
    lut: dict[str, list[tuple[str, str, str]]] = {}
    for r in p0.itertuples(index=False):
        k = r.playerName_norm
        if k:
            lut.setdefault(k, []).append((r.playerId, r.teamId, r.playerName))
    return lut

def resolve_player_id(raw_name: str, lut, alias_map, preferred_team: str | None):
    n = apply_alias(norm_name(raw_name), alias_map)
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
# clutch
# -----------------------------
def parse_clock_to_sec(clock: str) -> int | None:
    c = str(clock or "")
    if ":" not in c:
        return None
    try:
        mm, ss = c.split(":")
        return int(mm) * 60 + int(ss)
    except Exception:
        return None

def is_clutch(period_number, clock: str) -> bool:
    try:
        p = int(period_number)
    except Exception:
        return False
    sec = parse_clock_to_sec(clock)
    if sec is None:
        return False
    return (p == 4 and sec <= 120) or (p >= 5)


# -----------------------------
# foul parsing (fixed)
# -----------------------------
# captures full name before “... foul”
RE_FOUL_LEAD = re.compile(
    r"^(.+?)\s+(?:shooting|personal|offensive|technical|flagrant|loose ball|clear path|charge)?\s*foul\b",
    re.IGNORECASE
)

# finds parentheses chunks; we’ll pick the one that contains "draws the foul"
RE_PARENS = re.compile(r"\(([^()]*)\)")

# extracts the drawer name from: "NAME draws the foul"
RE_DRAWS = re.compile(r"^(.+?)\s+draws?\s+the\s+foul\b", re.IGNORECASE)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="pbp_events_canonical.csv")
    ap.add_argument("--phase0", required=True, help="phase0_players_index_2025.csv")
    ap.add_argument("--phase45", required=True, help="existing phase4_5_player_action_context_2025.csv")
    ap.add_argument("--season", default="2025")
    ap.add_argument("--aliases", default="", help="optional alias json/csv(raw,canon)")
    ap.add_argument("--out-delta", default="derived/phase4_5_foul_delta_2025.csv")
    ap.add_argument("--out-merged", default="derived/phase4_5_player_action_context_2025_merged_v2.csv")
    ap.add_argument("--out-unparsed", default="derived/phase4_5_unparsed_fouls_remaining.csv")
    ap.add_argument("--out-name-suggestions", default="derived/phase4_5_foul_name_suggestions.csv")
    args = ap.parse_args()

    events = pd.read_csv(args.events, low_memory=False)
    p0 = load_phase0(Path(args.phase0))
    lut = build_name_lookup(p0)

    alias_map = load_aliases(Path(args.aliases)) if args.aliases else {}

    team_att_col = "attribution_team_id" if "attribution_team_id" in events.columns else None

    rows = []
    unparsed = []
    not_found = []

    def add(pid: str, stat: str, inc: float):
        rows.append({"season_year": str(args.season), "player_id": str(pid), "stat": stat, "inc": float(inc)})

    for r in events.itertuples(index=False):
        et = str(getattr(r, "event_type", "") or "").lower()
        if "foul" not in et:
            continue

        desc = str(getattr(r, "description", "") or "")
        preferred_team = str(getattr(r, team_att_col)) if team_att_col else None
        clutch = is_clutch(getattr(r, "period_number", None), str(getattr(r, "clock", "") or ""))

        # fouler at the start
        m = RE_FOUL_LEAD.search(desc)
        fouler = m.group(1).strip() if m else None

        # drawer inside parentheses: "(NAME draws the foul)"
        drawer = None
        chunks = RE_PARENS.findall(desc)
        # choose the *last* chunk with draws the foul (handles "(Loose ball) (NAME draws the foul)")
        for ch in reversed(chunks):
            if "draw" in ch.lower() and "foul" in ch.lower():
                md = RE_DRAWS.search(ch.strip())
                if md:
                    drawer = md.group(1).strip()
                    break

        any_parsed = False

        if fouler:
            pid, nn, status = resolve_player_id(fouler, lut, alias_map, preferred_team)
            if pid:
                add(pid, "pf_committed", 1)
                if clutch:
                    add(pid, "clutch_pf_committed", 1)
                any_parsed = True
            else:
                not_found.append({"role":"fouler", "raw": fouler, "norm": nn, "status": status, "desc": desc})

        if drawer:
            pid, nn, status = resolve_player_id(drawer, lut, alias_map, preferred_team)
            if pid:
                add(pid, "pf_drawn", 1)
                if clutch:
                    add(pid, "clutch_pf_drawn", 1)
                any_parsed = True
            else:
                not_found.append({"role":"drawer", "raw": drawer, "norm": nn, "status": status, "desc": desc})

        if not any_parsed:
            unparsed.append({"event_type": et, "description": desc})

    # aggregate -> wide
    if rows:
        df = pd.DataFrame(rows)
        agg = df.groupby(["season_year","player_id","stat"], as_index=False)["inc"].sum()
        wide = agg.pivot_table(
            index=["season_year","player_id"],
            columns="stat",
            values="inc",
            aggfunc="sum",
            fill_value=0.0
        ).reset_index()
    else:
        wide = pd.DataFrame(columns=["season_year","player_id"])

    # attach canonical teamId (authoritative)
    wide = wide.merge(
        p0[["playerId","teamId"]].rename(columns={"playerId":"player_id","teamId":"team_id"}),
        on="player_id",
        how="left"
    )

    out_delta = Path(args.out_delta)
    out_delta.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(out_delta, index=False)

    out_unp = Path(args.out_unparsed)
    pd.DataFrame(unparsed).to_csv(out_unp, index=False)

    # suggestions for not_found
    p0_names = sorted(set(p0["playerName_norm"].tolist()))
    sug = []
    for item in not_found:
        n = item["norm"]
        if not n:
            continue
        matches = get_close_matches(n, p0_names, n=5, cutoff=0.86)
        if matches:
            for mn in matches:
                for pid, tid, orig in (lut.get(mn, [])[:3]):
                    sug.append({
                        "role": item["role"],
                        "raw": item["raw"],
                        "norm": n,
                        "suggest_playerName": orig,
                        "suggest_playerId": pid,
                        "suggest_teamId": tid
                    })
        else:
            sug.append({"role": item["role"], "raw": item["raw"], "norm": n, "suggest_playerName": "", "suggest_playerId":"", "suggest_teamId":""})

    out_sug = Path(args.out_name_suggestions)
    pd.DataFrame(sug).to_csv(out_sug, index=False)

    # merge into phase4.5 base
    base = pd.read_csv(args.phase45, low_memory=False)
    base["season_year"] = base.get("season_year", str(args.season)).astype(str)
    base["player_id"] = base["player_id"].astype(str)

    wide["season_year"] = wide["season_year"].astype(str)
    wide["player_id"] = wide["player_id"].astype(str)

    merged = base.merge(
        wide.drop(columns=["team_id"], errors="ignore"),
        on=["season_year","player_id"],
        how="left",
        suffixes=("", "_new")
    )

    # sum/rename new cols
    for c in [c for c in merged.columns if c.endswith("_new")]:
        base_name = c[:-4]
        merged[c] = merged[c].fillna(0)
        if base_name in merged.columns:
            merged[base_name] = merged[base_name].fillna(0) + merged[c]
            merged.drop(columns=[c], inplace=True)
        else:
            merged.rename(columns={c: base_name}, inplace=True)

    out_merged = Path(args.out_merged)
    merged.to_csv(out_merged, index=False)

    print("wrote delta:", out_delta, "rows:", len(wide))
    print("wrote merged:", out_merged, "rows:", len(merged))
    print("unparsed remaining:", out_unp, "rows:", len(unparsed))
    print("name suggestions:", out_sug, "rows:", len(sug))


if __name__ == "__main__":
    main()
