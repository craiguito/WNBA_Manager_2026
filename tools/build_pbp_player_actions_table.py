#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re
import unicodedata
import pandas as pd


# -----------------------------
# name normalization
# -----------------------------
SUFFIX_RE = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b\.?$", re.IGNORECASE)

def norm_name(s: str) -> str:
    if s is None or pd.isna(s):
        return ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
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
    df = pd.read_csv(path)
    if not {"raw","canon"} <= set(df.columns):
        raise RuntimeError("Alias CSV must have columns: raw, canon")
    return {norm_name(r.raw): norm_name(r.canon) for r in df.itertuples(index=False)}


# -----------------------------
# phase0 lookup
# -----------------------------
def load_phase0(phase0_csv: Path) -> pd.DataFrame:
    p0 = pd.read_csv(phase0_csv, low_memory=False)
    need = {"playerId", "playerName", "teamId"}
    missing = need - set(p0.columns)
    if missing:
        raise RuntimeError(f"phase0 missing {missing}. columns={p0.columns.tolist()}")
    p0 = p0.copy()
    p0["playerId"] = p0["playerId"].astype(str)
    p0["teamId"] = p0["teamId"].astype(str)
    p0["playerName_norm"] = p0["playerName"].map(norm_name)
    return p0


def build_name_lookup(p0: pd.DataFrame) -> dict[str, list[tuple[str,str,str]]]:
    lut = {}
    for r in p0.itertuples(index=False):
        k = r.playerName_norm
        if k:
            lut.setdefault(k, []).append((r.playerId, r.teamId, r.playerName))
    return lut


def resolve_player_id(raw_name: str, lut, alias_map, preferred_team: str | None) -> str | None:
    n = norm_name(raw_name)
    n = alias_map.get(n, n)
    cands = lut.get(n, [])
    if not cands:
        return None
    if len(cands) == 1:
        return cands[0][0]
    if preferred_team:
        for pid, tid, _ in cands:
            if tid == preferred_team:
                return pid
    # ambiguous -> refuse rather than guess
    return None


# -----------------------------
# time + coords
# -----------------------------
def clock_to_seconds(clock: str) -> int | None:
    c = str(clock or "")
    if ":" not in c:
        return None
    try:
        mm, ss = c.split(":")
        return int(mm) * 60 + int(ss)
    except Exception:
        return None


def normalize_xy_to_hoop(x, y):
    """
    Your coordinate ranges:
      x: 1..1128  (court width)
      y: 1..600   (court length)
    We normalize to half-court with hoop at (0,0).
    """
    try:
        x = float(x); y = float(y)
    except Exception:
        return None, None

    # centerline
    hx = x - 564.0

    # fold court so hoop baseline is always at y=0
    # if y in top half (>300), mirror around 600
    if y > 300.0:
        hy = 600.0 - y
    else:
        hy = y

    # hy now increases outward from hoop
    if hy < 0:
        hy = abs(hy)

    return hx, hy


# -----------------------------
# parsing patterns
# -----------------------------
RE_SHOT = re.compile(r"^(.+?)\s+(makes|misses)\s+(two point|three point)\b", re.IGNORECASE)
RE_FT = re.compile(r"^(.+?)\s+(makes|misses)\s+.*free throw\b", re.IGNORECASE)
RE_ASSIST = re.compile(r"\((.+?)\s+assists\)", re.IGNORECASE)

RE_REB = re.compile(r"^(.+?)\s+(offensive|defensive)\s+rebound\b", re.IGNORECASE)

RE_TOV_A = re.compile(r"^(.+?)\s+turnover\b", re.IGNORECASE)
RE_TOV_B = re.compile(r"turnover\s+by\s+(.+?)(?:\(|$)", re.IGNORECASE)

RE_STL = re.compile(r"\((.+?)\s+steals\)", re.IGNORECASE)
RE_BLK = re.compile(r"\((.+?)\s+blocks\)", re.IGNORECASE)

# fouls
RE_FOUL_LEAD = re.compile(r"^(.+?)\s+.*\bfoul\b", re.IGNORECASE)
RE_PARENS = re.compile(r"\(([^()]*)\)")
RE_DRAWS = re.compile(r"^(.+?)\s+draws?\s+the\s+foul\b", re.IGNORECASE)


def load_game_context(path: Path | None) -> pd.DataFrame | None:
    if not path or not path.exists():
        return None
    gc = pd.read_csv(path, low_memory=False)
    need = {"game_id","home_team_id","away_team_id"}
    missing = need - set(gc.columns)
    if missing:
        raise RuntimeError(f"game_context missing {missing}. columns={gc.columns.tolist()}")
    gc = gc.copy()
    gc["game_id"] = gc["game_id"].astype(str)
    gc["home_team_id"] = gc["home_team_id"].astype(str)
    gc["away_team_id"] = gc["away_team_id"].astype(str)
    return gc[["game_id","home_team_id","away_team_id"]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", required=True, help="pbp_events_canonical.csv")
    ap.add_argument("--phase0", required=True, help="phase0_players_index_2025.csv")
    ap.add_argument("--season", default="2025")
    ap.add_argument("--aliases", default="", help="optional alias json/csv(raw,canon)")
    ap.add_argument("--game_context", default="", help="optional phase4_game_context_2025.csv to compute team-perspective margin")
    ap.add_argument("--out", default="derived/pbp_player_actions_2025.csv")
    ap.add_argument("--issues_out", default="derived/pbp_player_actions_issues_2025.csv")
    args = ap.parse_args()

    ev = pd.read_csv(args.events, low_memory=False)
    p0 = load_phase0(Path(args.phase0))
    lut = build_name_lookup(p0)

    alias_map = load_aliases(Path(args.aliases)) if args.aliases else {}

    gc = load_game_context(Path(args.game_context)) if args.game_context else None
    if gc is not None:
        gc = gc.set_index("game_id")

    # columns used for team context (best effort)
    team_att_col = "attribution_team_id" if "attribution_team_id" in ev.columns else None
    poss_team_col = "possession_team_id" if "possession_team_id" in ev.columns else None

    out_rows = []
    issues = []

    def margin_for_team(game_id: str, team_id: str | None, home_pts: float, away_pts: float):
        # always compute margin_home
        margin_home = None
        if pd.notna(home_pts) and pd.notna(away_pts):
            margin_home = float(home_pts) - float(away_pts)

        if gc is None or not team_id or margin_home is None:
            return margin_home, None, None

        try:
            g = gc.loc[str(game_id)]
        except Exception:
            return margin_home, None, None

        home_id = str(g["home_team_id"])
        away_id = str(g["away_team_id"])

        if str(team_id) == home_id:
            m = margin_home
        elif str(team_id) == away_id:
            m = -margin_home
        else:
            return margin_home, None, None

        if m > 0:
            state = "winning"
        elif m < 0:
            state = "trailing"
        else:
            state = "tied"
        return margin_home, m, state

    def add_row(player_id, team_id, game_id, period, clock, et, action, result, points_value, hx, hy, home_pts, away_pts, desc, action_area, loc_x, loc_y):
        cs = clock_to_seconds(clock)
        margin_home, m_team, state = margin_for_team(game_id, team_id, home_pts, away_pts)

        out_rows.append({
            "season_year": str(args.season),
            "game_id": str(game_id),
            "player_id": str(player_id),
            "team_id": str(team_id) if team_id is not None else "",
            "period_number": int(period) if str(period).isdigit() else period,
            "clock": str(clock),
            "clock_seconds": cs,
            "event_type": str(et),
            "action": str(action),
            "result": str(result) if result is not None else "",
            "points_value": points_value if points_value is not None else "",
            "home_points": home_pts,
            "away_points": away_pts,
            "margin_home": margin_home if margin_home is not None else "",
            "margin_for_team": m_team if m_team is not None else "",
            "state": state if state is not None else "",
            "hx": hx if hx is not None else "",
            "hy": hy if hy is not None else "",
            "description": desc,
            "action_area": action_area if action_area is not None else "",
            "loc_x": loc_x if loc_x is not None else "",
            "loc_y": loc_y if loc_y is not None else "",
        })

    for r in ev.itertuples(index=False):
        game_id = str(getattr(r, "game_id", ""))
        period = getattr(r, "period_number", "")
        clock = str(getattr(r, "clock", "") or "")
        et = str(getattr(r, "event_type", "") or "").lower()
        desc = str(getattr(r, "description", "") or "")

        home_pts = getattr(r, "home_points", None)
        away_pts = getattr(r, "away_points", None)

        # coords
        loc_x = getattr(r, "loc_x", None) if "loc_x" in ev.columns else None
        loc_y = getattr(r, "loc_y", None) if "loc_y" in ev.columns else None
        action_area = getattr(r, "action_area", "") if "action_area" in ev.columns else ""

        hx, hy = normalize_xy_to_hoop(loc_x, loc_y) if (loc_x is not None and loc_y is not None) else (None, None)

        preferred_team = str(getattr(r, team_att_col)) if team_att_col else None
        poss_team = str(getattr(r, poss_team_col)) if poss_team_col else None

        # ---------------- shots (2/3) ----------------
        m = RE_SHOT.search(desc)
        if m:
            shooter = m.group(1).strip()
            verb = m.group(2).lower()
            kind = m.group(3).lower()

            pid = resolve_player_id(shooter, lut, alias_map, preferred_team)
            if not pid:
                issues.append({"bucket":"shot", "raw_name": shooter, "event_type": et, "description": desc})
            else:
                points_value = 3 if "three" in kind else 2
                add_row(
                    pid, preferred_team or poss_team, game_id, period, clock, et,
                    action=("three_pa" if points_value==3 else "two_pa"),
                    result=("made" if verb=="makes" else "missed"),
                    points_value=points_value,
                    hx=hx, hy=hy,
                    home_pts=home_pts, away_pts=away_pts,
                    desc=desc, action_area=action_area, loc_x=loc_x, loc_y=loc_y
                )

                # assist actor
                if verb == "makes":
                    a = RE_ASSIST.search(desc)
                    if a:
                        passer = a.group(1).strip()
                        apid = resolve_player_id(passer, lut, alias_map, preferred_team)
                        if apid:
                            add_row(
                                apid, preferred_team or poss_team, game_id, period, clock, et,
                                action="assist",
                                result="credited",
                                points_value="",
                                hx="", hy="",
                                home_pts=home_pts, away_pts=away_pts,
                                desc=desc, action_area="", loc_x="", loc_y=""
                            )
                        else:
                            issues.append({"bucket":"assist_name", "raw_name": passer, "event_type": et, "description": desc})
            continue

        # ---------------- free throws ----------------
        if ("freethrow" in et) or ("free_throw" in et) or ("free throw" in desc.lower()):
            mft = RE_FT.search(desc)
            if mft:
                shooter = mft.group(1).strip()
                verb = mft.group(2).lower()
            else:
                # looser fallback
                mft2 = re.match(r"^(.+?)\s+(makes|misses)\b", desc, flags=re.IGNORECASE)
                if not mft2 or "free throw" not in desc.lower():
                    continue
                shooter = mft2.group(1).strip()
                verb = mft2.group(2).lower()

            pid = resolve_player_id(shooter, lut, alias_map, preferred_team)
            if not pid:
                issues.append({"bucket":"ft", "raw_name": shooter, "event_type": et, "description": desc})
            else:
                add_row(
                    pid, preferred_team or poss_team, game_id, period, clock, et,
                    action="fta",
                    result=("made" if verb=="makes" else "missed"),
                    points_value=1,
                    hx="", hy="",  # FT coords are often placeholders
                    home_pts=home_pts, away_pts=away_pts,
                    desc=desc, action_area=action_area, loc_x=loc_x, loc_y=loc_y
                )
            continue

        # ---------------- rebounds ----------------
        mreb = RE_REB.search(desc)
        if mreb:
            name = mreb.group(1).strip()
            kind = mreb.group(2).lower()
            pid = resolve_player_id(name, lut, alias_map, preferred_team)
            if not pid:
                issues.append({"bucket":"rebound", "raw_name": name, "event_type": et, "description": desc})
            else:
                add_row(
                    pid, preferred_team or poss_team, game_id, period, clock, et,
                    action=("orb" if kind=="offensive" else "drb"),
                    result="secured",
                    points_value="",
                    hx="", hy="",
                    home_pts=home_pts, away_pts=away_pts,
                    desc=desc, action_area="", loc_x="", loc_y=""
                )
            continue

        # ---------------- turnovers ----------------
        if ("turnover" in et) or ("turnover" in desc.lower()):
            name = None
            mt = RE_TOV_A.search(desc)
            if mt:
                name = mt.group(1).strip()
            else:
                mt2 = RE_TOV_B.search(desc)
                if mt2:
                    name = mt2.group(1).strip()

            if name:
                pid = resolve_player_id(name, lut, alias_map, preferred_team)
                if not pid:
                    issues.append({"bucket":"turnover", "raw_name": name, "event_type": et, "description": desc})
                else:
                    add_row(
                        pid, preferred_team or poss_team, game_id, period, clock, et,
                        action="turnover",
                        result="committed",
                        points_value="",
                        hx="", hy="",
                        home_pts=home_pts, away_pts=away_pts,
                        desc=desc, action_area="", loc_x="", loc_y=""
                    )
            continue

        # ---------------- fouls (committed + drawn) ----------------
        if "foul" in et:
            mlead = RE_FOUL_LEAD.search(desc)
            fouler = mlead.group(1).strip() if mlead else None

            drawer = None
            chunks = RE_PARENS.findall(desc)
            for ch in reversed(chunks):
                if "draw" in ch.lower() and "foul" in ch.lower():
                    md = RE_DRAWS.search(ch.strip())
                    if md:
                        drawer = md.group(1).strip()
                        break

            if fouler:
                pid = resolve_player_id(fouler, lut, alias_map, preferred_team)
                if pid:
                    add_row(pid, preferred_team or poss_team, game_id, period, clock, et,
                            action="foul_committed", result="called", points_value="",
                            hx="", hy="", home_pts=home_pts, away_pts=away_pts,
                            desc=desc, action_area="", loc_x="", loc_y="")
                else:
                    issues.append({"bucket":"foul_committed", "raw_name": fouler, "event_type": et, "description": desc})

            if drawer:
                pid = resolve_player_id(drawer, lut, alias_map, preferred_team)
                if pid:
                    add_row(pid, preferred_team or poss_team, game_id, period, clock, et,
                            action="foul_drawn", result="drawn", points_value="",
                            hx="", hy="", home_pts=home_pts, away_pts=away_pts,
                            desc=desc, action_area="", loc_x="", loc_y="")
                else:
                    issues.append({"bucket":"foul_drawn", "raw_name": drawer, "event_type": et, "description": desc})

            continue

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(out_rows).to_csv(out, index=False)
    print("wrote:", out, "rows:", len(out_rows))

    issues_out = Path(args.issues_out)
    issues_out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(issues).to_csv(issues_out, index=False)
    print("issues:", issues_out, "rows:", len(issues))


if __name__ == "__main__":
    main()
