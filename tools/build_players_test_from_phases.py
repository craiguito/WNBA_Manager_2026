from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import numpy as np

BASE = Path(__file__).resolve().parents[1]
RAW = BASE / "raw_data"
OUT = BASE / "data" / "players_test.json"

# ---------- helpers ----------

def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def safe_div(num, den):
    if den is None or pd.isna(den) or den == 0:
        return np.nan
    return num / den


def pct_rank(series: pd.Series) -> pd.Series:
    s = series.copy()
    s = pd.to_numeric(s, errors="coerce")
    return s.rank(pct=True)


def rating(series: pd.Series) -> pd.Series:
    r = 25 + (pct_rank(series).fillna(0) * 74)
    r = r.clip(lower=25, upper=99)
    return r.round().astype(int)


def per36(stat: pd.Series, mpg: pd.Series) -> pd.Series:
    stat = pd.to_numeric(stat, errors="coerce")
    mpg = pd.to_numeric(mpg, errors="coerce")

    if not isinstance(stat, pd.Series):
        stat = pd.Series([stat])
    if not isinstance(mpg, pd.Series):
        mpg = pd.Series([mpg])

    if len(stat) == 1 and len(mpg) > 1:
        stat = pd.Series([stat.iloc[0]] * len(mpg), index=mpg.index)
    if len(mpg) == 1 and len(stat) > 1:
        mpg = pd.Series([mpg.iloc[0]] * len(stat), index=stat.index)

    idx = stat.index if len(stat) >= len(mpg) else mpg.index
    stat = stat.reindex(idx)
    mpg = mpg.reindex(idx)

    return pd.Series(np.where(mpg > 0, stat / (mpg / 36.0), np.nan), index=idx)


def split_name(name: str):
    parts = str(name).strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def norm_pos(pos: str) -> str:
    p = str(pos).upper()
    if "C" in p:
        return "C"
    if "F" in p:
        return "F"
    if "G" in p:
        return "G"
    return "G"


def archetype_from_top(fin, sho, play, deff, reb):
    top = max(
        [("Finisher", fin), ("Shooter", sho), ("Creator", play), ("Defender", deff), ("Rebounder", reb)],
        key=lambda x: x[1],
    )[0]
    return {
        "Finisher": "Two-Way Finisher",
        "Shooter": "Perimeter Shooter",
        "Creator": "Primary Creator",
        "Defender": "Defensive Specialist",
        "Rebounder": "Rebounding Big",
    }.get(top, "Two-Way Finisher")


def default_shot_shares(pos: str):
    if pos == "G":
        return 0.28, 0.18, 0.34
    if pos == "C":
        return 0.45, 0.18, 0.08
    return 0.32, 0.20, 0.22


# ---------- load data ----------

p0_path = RAW / "phase0_players_index_2025_with_bio.csv"
if not p0_path.exists():
    p0_path = RAW / "phase0_players_index_2025_merged.csv"

p0 = pd.read_csv(p0_path)

p1 = pd.read_csv(RAW / "phase1_players_workload_2025.csv")
p2_box = pd.read_csv(RAW / "phase2_players_box_2025.csv")
p2_shoot = pd.read_csv(RAW / "phase2_players_shooting_2025.csv")
p2_misc = pd.read_csv(RAW / "phase2_impact_misc_2025_rekeyed.csv")
p3 = pd.read_csv(RAW / "phase3_player_shot_profile_2025_rekeyed.csv")
p4 = pd.read_csv(RAW / "phase4_canonical" / "phase4_player_event_rates_2025_canonical.csv")

# ---------- phase4 aggregate ----------

p4_agg = (
    p4.groupby("player_id", as_index=False)
    .agg(
        games=("game_id", "nunique"),
        fga=("fga", "sum"),
        fta=("fta", "sum"),
        three_pa=("three_pa", "sum"),
        tov=("tov", "sum"),
        ast=("ast", "sum"),
        reb=("reb", "sum"),
        pf=("pf", "sum"),
        stl=("stl", "sum"),
        blk=("blk", "sum"),
        clutch_fga=("clutch_fga", "sum"),
        trans_fga=("trans_fga", "sum"),
        minutes_est=("minutes_est", "sum"),
        team_id=("team_id", "last"),
    )
)
p4_agg = p4_agg.rename(columns={"player_id": "playerId"})
p4_agg["mpg_p4"] = p4_agg["minutes_est"] / p4_agg["games"].replace({0: np.nan})
p4_agg["fga36_p4"] = per36(p4_agg["fga"], p4_agg["mpg_p4"])
p4_agg["three_pa36_p4"] = per36(p4_agg["three_pa"], p4_agg["mpg_p4"])
p4_agg["fta36_p4"] = per36(p4_agg["fta"], p4_agg["mpg_p4"])
p4_agg["ast36_p4"] = per36(p4_agg["ast"], p4_agg["mpg_p4"])
p4_agg["reb36_p4"] = per36(p4_agg["reb"], p4_agg["mpg_p4"])
p4_agg["stl36_p4"] = per36(p4_agg["stl"], p4_agg["mpg_p4"])
p4_agg["blk36_p4"] = per36(p4_agg["blk"], p4_agg["mpg_p4"])
p4_agg["tov36_p4"] = per36(p4_agg["tov"], p4_agg["mpg_p4"])
p4_agg["pf36_p4"] = per36(p4_agg["pf"], p4_agg["mpg_p4"])
p4_agg["clutch_rate"] = p4_agg["clutch_fga"] / p4_agg["fga"].replace({0: np.nan})
p4_agg["trans_rate"] = p4_agg["trans_fga"] / p4_agg["fga"].replace({0: np.nan})

# ---------- merge ----------

df = p0.copy()
for extra in (p1, p2_box, p2_shoot, p2_misc, p3, p4_agg):
    key = "playerId" if "playerId" in extra.columns else "player_id"
    if key != "playerId":
        extra = extra.rename(columns={key: "playerId"})
    df = df.merge(extra, on="playerId", how="left")

# fallback mpg/g from phase2 misc
if "mpg" not in df.columns:
    df["mpg"] = pd.NA
if "g" not in df.columns:
    df["g"] = pd.NA

if "mp" in df.columns:
    df["mpg"] = df["mpg"].fillna(df["mp"] / df["g"])  # mpg from total minutes

# ---------- core per36 stats ----------

fga36 = per36(df.get("fga"), df.get("mpg"))
fg3a36 = per36(df.get("fg3a"), df.get("mpg"))
fta36 = per36(df.get("fta"), df.get("mpg"))

ast36 = per36(df.get("ast"), df.get("mpg"))
trb36 = per36(df.get("trb"), df.get("mpg"))
orb36 = per36(df.get("orb"), df.get("mpg"))
stl36 = per36(df.get("stl"), df.get("mpg"))
blk36 = per36(df.get("blk"), df.get("mpg"))
tov36 = per36(df.get("tov"), df.get("mpg"))
pf36 = per36(df.get("pf"), df.get("mpg"))

# ---------- raw attribute signals ----------

finishing_raw = (fga36 - fg3a36) + (0.5 * fta36)
finishing_raw = (
    finishing_raw
    + (per36(df.get("rim_fga"), df.get("mpg")) * 0.4)
    + (per36(df.get("paint_fga"), df.get("mpg")) * 0.2)
    + (df.get("fga36_p4").fillna(0) * 0.3)
)
shooting_raw = (
    (pd.to_numeric(df.get("fg3Pct"), errors="coerce").fillna(0) * 0.6)
    + (pd.to_numeric(df.get("three_att_share"), errors="coerce").fillna(0) * 100 * 0.25)
    + (pd.to_numeric(df.get("three_fg"), errors="coerce").fillna(0) * 25 * 0.15)
    + (df.get("three_pa36_p4").fillna(0) * 0.4)
)
playmaking_raw = (
    ast36
    + (df.get("ast36_p4").fillna(0) * 0.6)
    - (0.6 * tov36)
    - (0.4 * df.get("tov36_p4").fillna(0))
    + (pd.to_numeric(df.get("usageProxyPer36"), errors="coerce").fillna(0) * 5)
)
defense_raw = (
    stl36
    + (1.2 * blk36)
    + (df.get("stl36_p4").fillna(0) * 0.6)
    + (df.get("blk36_p4").fillna(0) * 0.8)
    + (pd.to_numeric(df.get("onOff_plusMinus_per100"), errors="coerce").fillna(0) / 5)
)
rebounding_raw = trb36 + (0.5 * orb36) + (df.get("reb36_p4").fillna(0) * 0.6)
stamina_raw = pd.to_numeric(df.get("mpg"), errors="coerce").fillna(0) + (df.get("mpg_p4").fillna(0) * 0.6)

finishing = rating(finishing_raw)
shooting = rating(shooting_raw)
playmaking = rating(playmaking_raw)
defense = rating(defense_raw)
rebounding = rating(rebounding_raw)
stamina = rating(stamina_raw)

ovr = ((finishing + shooting + playmaking + defense + rebounding + stamina) / 6).round().astype(int)

# ---------- zone ratings ----------

rim_rating = rating(df.get("rim_fg"))
mid_rating = rating(df.get("mid_fg"))
three_rating = rating(df.get("three_fg"))

stl_rating = rating(stl36)
blk_rating = rating(blk36)

# ---------- build output ----------

players = []

for i, row in df.iterrows():
    player_id = row.get("playerId")
    name = row.get("playerName")
    first, last = split_name(name)
    pos = norm_pos(row.get("pos"))

    h = pd.to_numeric(row.get("heightIn"), errors="coerce")
    w = pd.to_numeric(row.get("weightLb"), errors="coerce")
    height = int(h) if pd.notna(h) else 72
    weight = float(w) if pd.notna(w) else 160.0

    fin = int(finishing.iloc[i])
    sho = int(shooting.iloc[i])
    ply = int(playmaking.iloc[i])
    dfn = int(defense.iloc[i])
    reb = int(rebounding.iloc[i])
    sta = int(stamina.iloc[i])

    rim_share = pd.to_numeric(row.get("rim_att_share"), errors="coerce")
    mid_share = pd.to_numeric(row.get("mid_att_share"), errors="coerce")
    three_share = pd.to_numeric(row.get("three_att_share"), errors="coerce")

    if pd.isna(rim_share) or pd.isna(mid_share) or pd.isna(three_share):
        rim_share, mid_share, three_share = default_shot_shares(pos)

    rim_share = clamp(float(rim_share), 0.02, 0.75)
    mid_share = clamp(float(mid_share), 0.02, 0.60)
    three_share = clamp(float(three_share), 0.02, 0.70)

    speed = int(round(clamp(95 - (height - 66) * 1.2, 60, 95)))
    acceleration = int(round(clamp(speed + 2, 60, 95)))
    lateral = int(round(clamp(speed - 2, 60, 95)))
    vertical = int(round(clamp(60 + (reb - 50) * 0.4, 55, 95)))
    strength = int(round(clamp(50 + (weight - 140) * 0.4, 50, 95)))
    wingspan = int(round(height + 6))

    discipline = int(round(clamp(100 - (pct_rank(pd.Series([tov36[i]])).iloc[0] * 50), 40, 95)))
    pf_mix = np.nanmean([pf36[i], row.get("pf36_p4")]) if pd.notna(row.get("pf36_p4")) else pf36[i]
    foul_tendency = clamp(float(pct_rank(pd.Series([pf_mix])).iloc[0] * 0.35), 0.08, 0.35)

    drive_bias = clamp(rim_share + ((fin - 50) / 200.0), 0.1, 0.8)
    pullup_bias = clamp(mid_share + ((sho - 50) / 300.0), 0.05, 0.6)
    ast_mix = np.nanmean([ast36[i], row.get("ast36_p4")]) if pd.notna(row.get("ast36_p4")) else ast36[i]
    kickout_bias = clamp(float(pct_rank(pd.Series([ast_mix])).iloc[0] * 0.6), 0.1, 0.7)
    postup_bias = clamp(((height - 68) / 20.0) + ((reb - 50) / 200.0), 0.05, 0.7)
    tov_mix = np.nanmean([tov36[i], row.get("tov36_p4")]) if pd.notna(row.get("tov36_p4")) else tov36[i]
    pass_risk = clamp(float(pct_rank(pd.Series([tov_mix])).iloc[0] * 0.6), 0.05, 0.7)

    g = pd.to_numeric(row.get("g"), errors="coerce")
    games = int(g) if pd.notna(g) else 0
    mpg_val = pd.to_numeric(row.get("mpg"), errors="coerce")
    mpg = float(mpg_val) if pd.notna(mpg_val) else 0.0
    minutes_est_val = pd.to_numeric(row.get("minutes_est"), errors="coerce")
    minutes_est = float(minutes_est_val) if pd.notna(minutes_est_val) else None
    trans_rate = pd.to_numeric(row.get("trans_rate"), errors="coerce")
    clutch_rate = pd.to_numeric(row.get("clutch_rate"), errors="coerce")
    if pd.isna(trans_rate):
        trans_rate = 0.0
    if pd.isna(clutch_rate):
        clutch_rate = 0.0

    players.append({
        "playerId": player_id,
        "firstName": first,
        "lastName": last,
        "displayName": name,
        "teamId": row.get("teamId") if pd.notna(row.get("teamId")) else row.get("team_id"),
        "position": pos,
        "heightIn": height,
        "weightLb": weight,
        "archetype": archetype_from_top(fin, sho, ply, dfn, reb),
        "badges": [],
        "attributes": {
            "finishing": fin,
            "shooting": sho,
            "playmaking": ply,
            "defense": dfn,
            "rebounding": reb,
            "stamina": sta,
        },
        "hiddenTraits": {
            "vision": round(clamp(ply / 100.0, 0.4, 0.9), 2),
            "bbiq": round(clamp(dfn / 100.0, 0.4, 0.9), 2),
            "composure": round(clamp(sho / 100.0, 0.35, 0.9), 2),
            "riskTolerance": round(clamp((row.get("usageProxyPer36") or 0) * 2, 0.2, 0.8), 2),
            "aggressionBias": round(clamp(fin / 100.0, 0.35, 0.9), 2),
        },
        "contract": {
            "yearsRemaining": 1,
            "salary": 100000,
        },
        "athleticProfile": {
            "speed": speed,
            "acceleration": acceleration,
            "lateralQuickness": lateral,
            "vertical": vertical,
            "strength": strength,
            "wingspanIn": wingspan,
        },
        "shootingProfile": {
            "rim": {"rating": int(rim_rating.iloc[i]), "tendency": round(rim_share, 2)},
            "midRange": {"rating": int(mid_rating.iloc[i]), "tendency": round(mid_share, 2)},
            "threePoint": {"rating": int(three_rating.iloc[i]), "tendency": round(three_share, 2)},
            "offDribble": int(round(0.6 * sho + 0.4 * ply)),
            "catchAndShoot": sho,
            "shotDiscipline": discipline,
        },
        "defenseProfile": {
            "onBall": dfn,
            "helpDefense": int(round((dfn + reb) / 2)),
            "stealTiming": int(stl_rating.iloc[i]),
            "blockTiming": int(blk_rating.iloc[i]),
            "closeoutControl": dfn,
            "foulTendency": round(foul_tendency, 2),
        },
        "mentalState": {
            "confidenceBaseline": round(clamp(sho / 100.0, 0.35, 0.85), 2),
            "pressureHandling": round(clamp((dfn + fin) / 200.0 + (clutch_rate * 0.2), 0.35, 0.92), 2),
            "clutchFactor": round(clamp((ply + sho) / 200.0 + (clutch_rate * 0.35), 0.3, 0.9), 2),
        },
        "healthProfile": {
            "durability": sta,
            "injuryRisk": round(clamp(0.4 - (pct_rank(pd.Series([games])).iloc[0] * 0.3), 0.08, 0.4), 2),
            "recoveryRate": round(clamp(0.5 + ((sta - 50) / 100.0), 0.4, 0.9), 2),
        },
        "tendencies": {
            "driveBias": round(drive_bias, 2),
            "pullUpBias": round(pullup_bias, 2),
            "kickOutBias": round(kickout_bias, 2),
            "postUpBias": round(postup_bias, 2),
            "passRiskTolerance": round(pass_risk, 2),
        },
        "roles": {
            "offensiveRoles": [],
            "defensiveRoles": [],
        },
        "handedness": {
            "primaryHand": "R",
        },
        "playtypeProfile": {
            "pnrBallhandler": {
                "frequency": round(clamp(0.12 + (ply / 400.0), 0.05, 0.45), 2),
                "efficiency": round(clamp((ply + fin) / 200.0, 0.35, 0.85), 2),
            },
            "pnrRollMan": {
                "frequency": round(clamp(0.05 + (reb / 400.0), 0.02, 0.35), 2),
                "efficiency": round(clamp((fin + reb) / 200.0, 0.35, 0.85), 2),
            },
            "spotUp": {
                "frequency": round(clamp(0.08 + (sho / 500.0), 0.05, 0.35), 2),
                "efficiency": round(clamp(sho / 100.0, 0.3, 0.8), 2),
            },
            "isolation": {
                "frequency": round(clamp(0.06 + (fin / 500.0), 0.05, 0.3), 2),
                "efficiency": round(clamp((fin + sho) / 200.0 + (clutch_rate * 0.15), 0.3, 0.85), 2),
            },
            "postUp": {
                "frequency": round(clamp(0.05 + (postup_bias * 0.3), 0.02, 0.35), 2),
                "efficiency": round(clamp((fin + reb) / 200.0, 0.35, 0.85), 2),
            },
            "transition": {
                "frequency": round(clamp(0.06 + (sta / 600.0) + (trans_rate * 0.5), 0.05, 0.5), 2),
                "efficiency": round(clamp((fin + sta) / 200.0, 0.35, 0.85), 2),
            },
        },
        "seasonState": {
            "fatigueCurrent": round(clamp((mpg / 40.0) * 0.35, 0.05, 0.35), 2),
            "minutesLoad7d": int(round(mpg * 3.5)),
            "minutesLoadSeason": int(round(minutes_est)) if minutes_est is not None else int(round(mpg * games)),
            "injuryStatus": {
                "isInjured": False,
                "type": None,
                "severity": 0,
                "expectedDaysOut": 0,
                "playingLimited": False,
            },
        },
        "decisionTuning": {
            "processingSpeed": round(clamp(ply / 100.0, 0.4, 0.95), 2),
            "consistency": round(clamp(ovr.iloc[i] / 100.0, 0.4, 0.95), 2),
        },
    })

    # roles (post-build to use ratings)
    if players[-1]["attributes"]["playmaking"] >= 80:
        players[-1]["roles"]["offensiveRoles"].append("PrimaryBallhandler")
    else:
        players[-1]["roles"]["offensiveRoles"].append("SecondaryBallhandler")

    if players[-1]["attributes"]["shooting"] >= 80:
        players[-1]["roles"]["offensiveRoles"].append("SpotUp")

    if players[-1]["attributes"]["finishing"] >= 80:
        players[-1]["roles"]["offensiveRoles"].append("TransitionFinisher")

    if players[-1]["attributes"]["rebounding"] >= 80 or pos in ("C", "F"):
        players[-1]["roles"]["offensiveRoles"].append("Roller")

    if players[-1]["attributes"]["defense"] >= 80:
        if pos == "G":
            players[-1]["roles"]["defensiveRoles"].append("PointOfAttack")
        else:
            players[-1]["roles"]["defensiveRoles"].append("RimProtector")
    else:
        players[-1]["roles"]["defensiveRoles"].append("Helper")

# ---------- write ----------

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(players, f, indent=2)

print(f"Wrote {len(players)} players to {OUT}")



