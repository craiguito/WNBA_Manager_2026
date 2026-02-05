#!/usr/bin/env python3
import re
import unicodedata
from pathlib import Path
import pandas as pd


PHASE0_IN = Path("raw_data/phase0_players_index_2025.csv")
PHASE0_OUT = Path("raw_data/phase0_players_index_2025_with_age.csv")

PHASE3_IN = Path("raw_data/phase3_shot_locations_2025.csv")
PHASE3_OUT = Path("raw_data/phase3_player_shot_profile_2025_rekeyed.csv")


def norm_name(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_")
    )
    return df


def pick_col(df: pd.DataFrame, candidates: list[str]) -> str:
    """Return the first candidate that exists, else raise with helpful info."""
    for c in candidates:
        if c in df.columns:
            return c
    raise KeyError(
        "Missing expected column. Tried: "
        + ", ".join(candidates)
        + "\nColumns available (first 60): "
        + ", ".join(df.columns.tolist()[:60])
    )


def safe_div(num: pd.Series, den: pd.Series) -> pd.Series:
    return num / den.replace({0: pd.NA})


def main():
    if not PHASE0_IN.exists():
        raise FileNotFoundError(f"missing {PHASE0_IN}")
    if not PHASE3_IN.exists():
        raise FileNotFoundError(f"missing {PHASE3_IN}")

    p0 = clean_cols(pd.read_csv(PHASE0_IN))
    s3 = clean_cols(pd.read_csv(PHASE3_IN))

    # phase0 required
    p0_name = pick_col(p0, ["playername", "player_name", "name"])
    p0_pid  = pick_col(p0, ["playerid", "player_id"])
    p0_tid  = pick_col(p0, ["teamid", "team_id"])
    p0_pos  = "pos" if "pos" in p0.columns else ("position" if "position" in p0.columns else None)

    # phase3 required name (WNBA stats name)
    s3_name = pick_col(s3, ["player_name", "playername", "name", "player"])

    # OPTIONAL: if your phase3 already has age, we’ll use it to fill phase0
    s3_age = None
    for c in ["age", "player_age", "age_years"]:
        if c in s3.columns:
            s3_age = c
            break

    # --- auto-detect bucket columns (this avoids your KeyError) ---
    # rim / restricted area
    rim_fgm = pick_col(s3, ["restricted_area_fgm", "restricted_area_fg_m", "restrictedarea_fgm"])
    rim_fga = pick_col(s3, ["restricted_area_fga", "restricted_area_fg_a", "restrictedarea_fga"])

    # paint non-RA (this is the one that bit you)
    paint_fgm = pick_col(s3, [
        "paint_non_ra_fgm",
        "in_the_paint_non_ra_fgm",
        "in_the_paint_non_ra_fg_m",
        "paint_non_ra_fg_m",
    ])
    paint_fga = pick_col(s3, [
        "paint_non_ra_fga",
        "in_the_paint_non_ra_fga",
        "in_the_paint_non_ra_fg_a",
        "paint_non_ra_fg_a",
    ])

    # mid-range
    mid_fgm = pick_col(s3, ["mid_range_fgm", "mid_range_fg_m", "midrange_fgm"])
    mid_fga = pick_col(s3, ["mid_range_fga", "mid_range_fg_a", "midrange_fga"])

    # corner 3
    c3_fgm = pick_col(s3, ["corner_3_fgm", "corner3_fgm", "corner_3_fg_m"])
    c3_fga = pick_col(s3, ["corner_3_fga", "corner3_fga", "corner_3_fg_a"])

    # above the break 3
    ab3_fgm = pick_col(s3, ["above_break_3_fgm", "above_the_break_3_fgm", "ab3_fgm"])
    ab3_fga = pick_col(s3, ["above_break_3_fga", "above_the_break_3_fga", "ab3_fga"])

    # --- keys for joining ---
    p0["namekey"] = p0[p0_name].apply(norm_name)
    s3["namekey"] = s3[s3_name].apply(norm_name)

    # --- build phase3 profile ---
    prof = pd.DataFrame({
        "namekey": s3["namekey"],
        "playerName": s3[s3_name],
        "rim_fgm": s3[rim_fgm],
        "rim_fga": s3[rim_fga],
        "paint_fgm": s3[paint_fgm],
        "paint_fga": s3[paint_fga],
        "mid_fgm": s3[mid_fgm],
        "mid_fga": s3[mid_fga],
        "corner3_fgm": s3[c3_fgm],
        "corner3_fga": s3[c3_fga],
        "ab3_fgm": s3[ab3_fgm],
        "ab3_fga": s3[ab3_fga],
    })

    # totals + pcts + shares
    prof["three_fgm"] = prof["corner3_fgm"] + prof["ab3_fgm"]
    prof["three_fga"] = prof["corner3_fga"] + prof["ab3_fga"]
    prof["total_fgm"] = prof["rim_fgm"] + prof["paint_fgm"] + prof["mid_fgm"] + prof["three_fgm"]
    prof["total_fga"] = prof["rim_fga"] + prof["paint_fga"] + prof["mid_fga"] + prof["three_fga"]

    for z in ["rim", "paint", "mid", "corner3", "ab3", "three", "total"]:
        prof[f"{z}_fg"] = safe_div(prof[f"{z}_fgm"], prof[f"{z}_fga"])

    for z in ["rim", "paint", "mid", "corner3", "ab3", "three"]:
        prof[f"{z}_att_share"] = safe_div(prof[f"{z}_fga"], prof["total_fga"]).fillna(0)

    # attach age if present
    if s3_age:
        prof["age"] = pd.to_numeric(s3[s3_age], errors="coerce")

    # --- merge age into phase0 (from phase3 if present) ---
    p0_out = p0.copy()
    if "age" not in p0_out.columns:
        p0_out["age"] = pd.NA

    if s3_age:
        age_map = prof[["namekey", "age"]].dropna().drop_duplicates("namekey")
        p0_out = p0_out.drop(columns=["age"]).merge(age_map, on="namekey", how="left")

    p0_out.drop(columns=["namekey"], inplace=True)
    p0_out.to_csv(PHASE0_OUT, index=False, encoding="utf-8")
    print(f"✅ wrote {PHASE0_OUT}")

    # --- rekey phase3 to canonical playerId/teamId (and kill TOT) ---
    canon = p0.copy()
    canon = canon[["namekey", p0_pid, p0_tid] + ([p0_pos] if p0_pos else [])].copy()
    canon.rename(columns={p0_pid: "playerId", p0_tid: "teamId"}, inplace=True)

    rekeyed = prof.merge(canon, on="namekey", how="left", validate="one_to_one")

    # if TOT ever appears in phase3 side, we force teamId from canon anyway
    rekeyed.drop(columns=["namekey"], inplace=True)
    rekeyed.to_csv(PHASE3_OUT, index=False, encoding="utf-8")

    cov = rekeyed["playerId"].notna().sum()
    print(f"✅ wrote {PHASE3_OUT} (rekey coverage {cov}/{len(rekeyed)})")

    # show a few misses
    misses = rekeyed[rekeyed["playerId"].isna()][["playerName"]].head(15)
    if len(misses):
        print("⚠️ sample unmatched names (first 15):")
        for n in misses["playerName"].tolist():
            print("  -", n)


if __name__ == "__main__":
    main()