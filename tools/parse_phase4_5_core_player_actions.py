#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re
import unicodedata
import pandas as pd


# -----------------------------
# name normalization + matching
# -----------------------------
def norm_name(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    # strip accents
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    # remove punctuation
    s = re.sub(r"[^\w\s'-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def build_player_lookup(phase0_csv: Path):
    p0 = pd.read_csv(phase0_csv, low_memory=False)
    required = {"playerId", "playerName", "teamId"}
    missing = required - set(p0.columns)
    if missing:
        raise RuntimeError(f"phase0 missing {missing}. columns: {p0.columns.tolist()}")

    p0 = p0.copy()
    p0["playerName_norm"] = p0["playerName"].map(norm_name)

    # name -> list of candidates (playerId, teamId, originalName)
    lut = {}
    for _, r in p0.iterrows():
        key = r["playerName_norm"]
        if not key:
            continue
        lut.setdefault(key, []).append(
            (str(r["playerId"]), str(r["teamId"]), str(r["playerName"]))
        )
    return lut


def resolve_name_to_playerId(
    raw_name: str,
    name_lut: dict,
    preferred_team: str | None,
    issues: list,
    ctx: dict,
    role: str,
):
    """
    Resolve a raw player name string to canonical playerId using:
    - exact normalized name match
    - if multiple players share a normalized name (rare), pick candidate on preferred_team if provided
    """
    n = norm_name(raw_name)
    if not n:
        issues.append({**ctx, "role": role, "raw_name": raw_name, "problem": "empty_name"})
        return None

    candidates = name_lut.get(n, [])
    if not candidates:
        issues.append({**ctx, "role": role, "raw_name": raw_name, "problem": "name_not_found"})
        return None

    if len(candidates) == 1:
        return candidates[0][0]

    # multiple candidates: try team context
    if preferred_team:
        for pid, tid, _ in candidates:
            if tid == preferred_team:
                return pid

    # fallback: take first deterministically but log ambiguity
    issues.append({**ctx, "role": role, "raw_name": raw_name, "problem": f"ambiguous_name({len(candidates)})"})
    return candidates[0][0]


# -----------------------------
# parsing description patterns
# -----------------------------
RE_SHOT = re.compile(r"^(.+?)\s+(makes|misses)\s+(two point|three point|free throw)", re.IGNORECASE)
RE_ASSIST = re.compile(r"\((.+?)\s+assists\)", re.IGNORECASE)
RE_BLOCK_PAREN = re.compile(r"\((.+?)\s+blocks\)", re.IGNORECASE)
RE_STEAL_PAREN = re.compile(r"\((.+?)\s+steals\)", re.IGNORECASE)

RE_BLOCK_BY = re.compile(r"blocked\s+by\s+(.+?)(?:\)|$)", re.IGNORECASE)
RE_STEAL_BY = re.compile(r"steal\s+by\s+(.+?)(?:\)|$)", re.IGNORECASE)

RE_TOV = re.compile(r"^(.+?)\s+turnover", re.IGNORECASE)
RE_FOUL_BY = re.compile(r"foul\s+by\s+(.+?)(?:\(|$)", re.IGNORECASE)
RE_FOUL_DRAWN_BY = re.compile(r"drawn\s+by\s+(.+?)(?:\(|$)", re.IGNORECASE)


def is_transition(row) -> int:
    q = str(row.get("qualifiers_joined", "") or "").lower()
    d = str(row.get("description", "") or "").lower()
    return int(("transition" in q) or ("fast break" in q) or ("fastbreak" in q) or ("fast break" in d) or ("fastbreak" in d))


def is_clutch(row) -> int:
    # clutch = last 2:00 of 4th or any OT
    period = row.get("period_number", None)
    clock = str(row.get("clock", "") or "")
    try:
        if ":" in clock:
            mm, ss = clock.split(":")
            sec = int(mm) * 60 + int(ss)
        else:
            return 0
        if str(period).isdigit():
            p = int(period)
            if (p == 4 and sec <= 120) or (p >= 5):
                return 1
    except Exception:
        return 0
    return 0


def zone_from_action_area(row) -> str:
    area = str(row.get("action_area", "") or "").lower()
    if "restricted" in area or "rim" in area:
        return "rim"
    if "paint" in area:
        return "paint"
    if "mid" in area:
        return "mid"
    if "corner" in area and "3" in area:
        return "corner3"
    if "above" in area or "break" in area:
        return "ab3"
    # sometimes action_area might just say "three point" etc
    if "3" in area or "three" in area:
        return "three_unknown"
    return "unknown"


# -----------------------------
# main extraction
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Phase 4.5: Parse pbp_events_canonical into core player actions.")
    ap.add_argument("--events", required=True, help="pbp_events_canonical.csv")
    ap.add_argument("--phase0", required=True, help="phase0_players_index_2025.csv")
    ap.add_argument("--season", default="2025", help="season_year label")
    ap.add_argument("--out", required=True, help="output csv (derived/phase4_5_player_action_context_2025.csv)")
    ap.add_argument("--issues-out", default="derived/phase4_5_player_action_context_issues.csv")
    args = ap.parse_args()

    events = pd.read_csv(args.events, low_memory=False)
    name_lut = build_player_lookup(Path(args.phase0))

    required = {"game_id", "event_id", "event_type", "description", "period_number", "clock"}
    missing = required - set(events.columns)
    if missing:
        raise RuntimeError(f"events missing {missing}. columns: {events.columns.tolist()}")

    # team context columns (best effort)
    # attribution_team_id is your canonical team alias (IND, SEA, etc) after rekey.
    team_col = "attribution_team_id" if "attribution_team_id" in events.columns else None
    poss_team_col = "possession_team_id" if "possession_team_id" in events.columns else None

    issues = []

    # per-player accumulator
    rows = []

    def add_stat(player_id: str, team_id: str | None, stat: str, inc: float, ctx: dict):
        rows.append({
            "player_id": player_id,
            "team_id": team_id if team_id else "",
            "stat": stat,
            "inc": inc,
            "season_year": args.season,
            "game_id": ctx["game_id"],
        })

    for _, r in events.iterrows():
        et = str(r.get("event_type", "") or "").lower()
        desc = str(r.get("description", "") or "")
        ctx = {
            "game_id": str(r.get("game_id")),
            "event_id": str(r.get("event_id")),
            "event_type": et,
        }

        team_att = str(r.get(team_col)) if team_col else None
        team_poss = str(r.get(poss_team_col)) if poss_team_col else None

        clutch = is_clutch(r)
        trans = is_transition(r)
        zone = zone_from_action_area(r)

        # -----------------
        # SHOTS (2pt/3pt/FT) + assists in parentheses
        # -----------------
        m = RE_SHOT.search(desc)
        if m:
            shooter_name = m.group(1).strip()
            verb = m.group(2).lower().strip()    # makes/misses
            shot_kind = m.group(3).lower().strip()  # two point / three point / free throw

            # shooter should belong to attribution team if present; else use possession team
            preferred_team = team_att if team_att else team_poss
            shooter_id = resolve_name_to_playerId(shooter_name, name_lut, preferred_team, issues, ctx, "shooter")
            if shooter_id:
                made = 1 if verb == "makes" else 0

                if shot_kind == "free throw":
                    add_stat(shooter_id, preferred_team, "fta", 1, ctx)
                    if made:
                        add_stat(shooter_id, preferred_team, "ftm", 1, ctx)
                    # clutch/trans on FTs not super useful but keep clutch for pressure
                    if clutch:
                        add_stat(shooter_id, preferred_team, "clutch_fta", 1, ctx)
                else:
                    add_stat(shooter_id, preferred_team, "fga", 1, ctx)
                    if made:
                        add_stat(shooter_id, preferred_team, "fgm", 1, ctx)

                    if shot_kind == "three point":
                        add_stat(shooter_id, preferred_team, "three_pa", 1, ctx)
                        if made:
                            add_stat(shooter_id, preferred_team, "three_pm", 1, ctx)
                    else:
                        add_stat(shooter_id, preferred_team, "two_pa", 1, ctx)
                        if made:
                            add_stat(shooter_id, preferred_team, "two_pm", 1, ctx)

                    # zone attempts/makes
                    add_stat(shooter_id, preferred_team, f"zone_{zone}_att", 1, ctx)
                    if made:
                        add_stat(shooter_id, preferred_team, f"zone_{zone}_made", 1, ctx)

                    # context flags for shots
                    if clutch:
                        add_stat(shooter_id, preferred_team, "clutch_fga", 1, ctx)
                    if trans:
                        add_stat(shooter_id, preferred_team, "trans_fga", 1, ctx)

                # assisted vs unassisted (only for made field goals)
                a = RE_ASSIST.search(desc)
                if a and shot_kind != "free throw" and made:
                    assister_name = a.group(1).strip()
                    assister_id = resolve_name_to_playerId(assister_name, name_lut, preferred_team, issues, ctx, "assister")
                    if assister_id:
                        add_stat(assister_id, preferred_team, "ast", 1, ctx)
                        add_stat(shooter_id, preferred_team, "assisted_fgm", 1, ctx)
                elif shot_kind != "free throw" and made:
                    add_stat(shooter_id, preferred_team, "unassisted_fgm", 1, ctx)

            continue  # done with shot event

        # -----------------
        # TURNOVERS
        # -----------------
        if "turnover" in et or et == "turnover":
            m2 = RE_TOV.search(desc)
            if m2:
                p_name = m2.group(1).strip()
                preferred_team = team_att if team_att else team_poss
                pid = resolve_name_to_playerId(p_name, name_lut, preferred_team, issues, ctx, "turnover_player")
                if pid:
                    add_stat(pid, preferred_team, "tov", 1, ctx)
                    # turnover subtype if present in column
                    tov_type = str(r.get("turnover_type", "") or "").lower()
                    if "bad" in tov_type and "pass" in tov_type:
                        add_stat(pid, preferred_team, "tov_bad_pass", 1, ctx)
                    elif "lost" in tov_type and "ball" in tov_type:
                        add_stat(pid, preferred_team, "tov_lost_ball", 1, ctx)
                    elif tov_type:
                        add_stat(pid, preferred_team, f"tov_{re.sub(r'[^a-z0-9]+','_',tov_type).strip('_')}", 1, ctx)

                    if clutch:
                        add_stat(pid, preferred_team, "clutch_tov", 1, ctx)
            else:
                issues.append({**ctx, "role": "turnover_player", "raw_name": "", "problem": "could_not_parse_turnover"})
            continue

        # -----------------
        # STEALS / BLOCKS (may be in parentheses or "by NAME")
        # -----------------
        if "steal" in et:
            name = None
            m3 = RE_STEAL_PAREN.search(desc)
            if m3:
                name = m3.group(1).strip()
            else:
                m3b = RE_STEAL_BY.search(desc)
                if m3b:
                    name = m3b.group(1).strip()
            if name:
                # stealer is usually the *defense*, often opposite possession team
                preferred_team = None
                if team_poss and team_att:
                    # if attribution is offense, stealer defense = opposite
                    # but sometimes attribution_team_id already equals defense depending on feed
                    # so: if description includes "steal", prefer NOT possession team when ambiguous
                    preferred_team = None
                pid = resolve_name_to_playerId(name, name_lut, preferred_team, issues, ctx, "stealer")
                if pid:
                    add_stat(pid, "", "stl", 1, ctx)
            continue

        if "block" in et:
            name = None
            m4 = RE_BLOCK_PAREN.search(desc)
            if m4:
                name = m4.group(1).strip()
            else:
                m4b = RE_BLOCK_BY.search(desc)
                if m4b:
                    name = m4b.group(1).strip()
            if name:
                pid = resolve_name_to_playerId(name, name_lut, None, issues, ctx, "blocker")
                if pid:
                    add_stat(pid, "", "blk", 1, ctx)
            continue

        # -----------------
        # FOULS (committed + drawn if text has it)
        # -----------------
        if "foul" in et:
            fouler = None
            drawn = None

            mf = RE_FOUL_BY.search(desc)
            if mf:
                fouler = mf.group(1).strip()

            md = RE_FOUL_DRAWN_BY.search(desc)
            if md:
                drawn = md.group(1).strip()

            # sometimes description is like: "NAME personal foul"
            if not fouler:
                # try leading-name pattern
                lead = re.match(r"^(.+?)\s+.*foul", desc, flags=re.IGNORECASE)
                if lead:
                    fouler = lead.group(1).strip()

            if fouler:
                pid = resolve_name_to_playerId(fouler, name_lut, None, issues, ctx, "fouler")
                if pid:
                    add_stat(pid, "", "pf_committed", 1, ctx)

            if drawn:
                pid2 = resolve_name_to_playerId(drawn, name_lut, None, issues, ctx, "fouled_player")
                if pid2:
                    add_stat(pid2, "", "pf_drawn", 1, ctx)

            # if neither parsed, log
            if not fouler and not drawn:
                issues.append({**ctx, "role": "foul", "raw_name": "", "problem": "could_not_parse_foul"})
            continue

        # (optional) rebounds etc could be added later

    if not rows:
        raise RuntimeError("No rows produced. Check file paths or parsing patterns.")

    df = pd.DataFrame(rows)

# Aggregate per player/team/season/stat
    agg = df.groupby(["season_year", "player_id", "team_id", "stat"], as_index=False)["inc"].sum()

# pivot stats wide
    wide = agg.pivot_table(
        index=["season_year", "player_id", "team_id"],
        columns="stat",
        values="inc",
        aggfunc="sum",
        fill_value=0.0,
        ).reset_index()
    wide.columns = [c if isinstance(c, str) else str(c) for c in wide.columns]


    # derived rates using minutes later in mart; here just compute a few safe ratios
    if "fga" in wide.columns:
        wide["three_rate"] = (wide.get("three_pa", 0) / wide["fga"]).where(wide["fga"] > 0, 0.0)
        wide["fta_rate"] = (wide.get("fta", 0) / wide["fga"]).where(wide["fga"] > 0, 0.0)
        wide["clutch_fga_share"] = (wide.get("clutch_fga", 0) / wide["fga"]).where(wide["fga"] > 0, 0.0)
        wide["trans_fga_share"] = (wide.get("trans_fga", 0) / wide["fga"]).where(wide["fga"] > 0, 0.0)

    if "ast" in wide.columns:
        wide["ast_to_tov"] = (wide["ast"] / (wide.get("tov", 0) + 1)).astype(float)

    # write outputs
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(out_path, index=False)

    issues_path = Path(args.issues_out)
    issues_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(issues).to_csv(issues_path, index=False)

    print("wrote:", out_path, "rows:", len(wide))
    print("issues:", issues_path, "rows:", len(issues))


if __name__ == "__main__":
    main()
