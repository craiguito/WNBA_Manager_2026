#!/usr/bin/env python3
import re
import unicodedata
from pathlib import Path
import pandas as pd
import argparse


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


def find_first_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase0", default="raw_data/phase0_players_index_2025.csv")
    ap.add_argument("--phase3", default="raw_data/phase3_player_shot_profile_2025.csv")
    ap.add_argument("--phase0_out", default="raw_data/phase0_players_index_2025_with_age.csv")
    ap.add_argument("--phase3_out", default="raw_data/phase3_player_shot_profile_2025_rekeyed.csv")
    args = ap.parse_args()

    phase0_path = Path(args.phase0)
    phase3_path = Path(args.phase3)

    if not phase0_path.exists():
        raise FileNotFoundError(f"missing {phase0_path}")
    if not phase3_path.exists():
        raise FileNotFoundError(f"missing {phase3_path}")

    # Load
    p0 = pd.read_csv(phase0_path)
    s3 = pd.read_csv(phase3_path)

    p0 = clean_cols(p0)
    s3 = clean_cols(s3)

    # Detect columns
    p0_name = find_first_col(p0, ["playername", "player_name", "name"])
    p0_pid  = find_first_col(p0, ["playerid", "player_id"])
    p0_tid  = find_first_col(p0, ["teamid", "team_id"])
    p0_pos  = find_first_col(p0, ["pos", "position"])

    s3_name = find_first_col(s3, ["playername", "player_name", "name"])
    s3_pid  = find_first_col(s3, ["playerid", "player_id"])
    s3_tid  = find_first_col(s3, ["teamid", "team_id"])
    s3_age  = find_first_col(s3, ["age", "player_age", "age_years"])

    required_p0 = [p0_name, p0_pid, p0_tid]
    if any(c is None for c in required_p0):
        raise RuntimeError(
            f"phase0 missing required cols. need playerName/playerId/teamId. "
            f"found cols: {p0.columns.tolist()[:40]}"
        )

    if s3_name is None:
        raise RuntimeError(
            f"phase3 missing playerName column. found cols: {s3.columns.tolist()[:40]}"
        )
    if s3_age is None:
        raise RuntimeError(
            f"phase3 missing age column (age/player_age/age_years). found cols: {s3.columns.tolist()[:40]}"
        )

    # Normalize keys
    p0["namekey"] = p0[p0_name].apply(norm_name)
    s3["namekey"] = s3[s3_name].apply(norm_name)

    # ---- 1) merge AGE into phase0 ----
    age_map = s3[["namekey", s3_age]].copy()
    age_map.rename(columns={s3_age: "age"}, inplace=True)
    age_map["age"] = pd.to_numeric(age_map["age"], errors="coerce")

    # if duplicates, keep the most non-null / latest row
    age_map = age_map.sort_values("age").drop_duplicates("namekey", keep="last")

    p0_with_age = p0.merge(age_map, on="namekey", how="left", validate="one_to_one")

    # write phase0_with_age
    out0 = p0_with_age.drop(columns=["namekey"])
    out0.to_csv(args.phase0_out, index=False, encoding="utf-8")
    print(f"✅ wrote {args.phase0_out} (age coverage: {out0['age'].notna().sum()}/{len(out0)})")

    # ---- 2) rekey phase3 playerId + replace TOT teamId ----
    # Build canonical mapping from phase0
    map_cols = [p0_pid, p0_name, p0_tid]
    if p0_pos:
        map_cols.append(p0_pos)

    canon = p0_with_age[["namekey"] + map_cols + ["age"]].copy()
    canon.rename(
        columns={
            p0_pid: "playerId_canon",
            p0_tid: "teamId_canon",
            p0_name: "playerName_canon",
            p0_pos if p0_pos else "": "pos_canon",
        },
        inplace=True,
    )
    # if pos_canon rename created empty col name, ignore
    canon = canon.loc[:, ~canon.columns.duplicated()]

    s3_rekey = s3.merge(canon, on="namekey", how="left", validate="one_to_one")

    # overwrite / create canonical ids
    s3_rekey["playerid"] = s3_rekey["playerId_canon"]
    s3_rekey["teamid"] = s3_rekey["teamId_canon"]

    # if phase3 originally had TOT-like teamid, this ensures it’s replaced
    # also bring canon age if you prefer canon (should match anyway)
    s3_rekey["age"] = s3_rekey["age_y"] if "age_y" in s3_rekey.columns else s3_rekey.get("age", pd.NA)
    if "age_x" in s3_rekey.columns and "age_y" in s3_rekey.columns:
        # prefer canon age when available
        s3_rekey["age"] = s3_rekey["age_y"].combine_first(s3_rekey["age_x"])

    # clean up helper columns
    drop_helpers = [c for c in [
        "namekey",
        "playerId_canon", "teamId_canon", "playerName_canon",
        "pos_canon",
        "age_x", "age_y",
    ] if c in s3_rekey.columns]
    s3_rekey.drop(columns=drop_helpers, inplace=True)

    # write
    s3_rekey.to_csv(args.phase3_out, index=False, encoding="utf-8")
    cov = s3_rekey["playerid"].notna().sum()
    print(f"✅ wrote {args.phase3_out} (rekey coverage: {cov}/{len(s3_rekey)})")

    # show a few misses
    misses = s3[s3["namekey"].isin(set(p0["namekey"])) == False]
    # above line too strict; instead: show where canon id missing
    if "playerid" in s3_rekey.columns:
        miss_rows = s3_rekey[s3_rekey["playerid"].isna()]
        if len(miss_rows):
            print("⚠️ sample unmatched players (first 20):")
            show = miss_rows[[s3_name]].head(20)
            for _, r in show.iterrows():
                print(f"  - {r[s3_name]}")


if __name__ == "__main__":
    main()