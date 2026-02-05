import pandas as pd
from pathlib import Path
import re
import unicodedata

PHASE0_PATH = Path("raw_data/phase0_players_index_2025.csv")
AGE_PATH = Path("raw_data/phase0_players_index_2025_with_age.csv")
OUT_PATH = Path("raw_data/phase0_players_index_2025_merged.csv")


def norm_name(s: str) -> str:
    if pd.isna(s):
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main():
    p0 = pd.read_csv(PHASE0_PATH)
    age_df = pd.read_csv(AGE_PATH)

    # normalize age file columns to expected names
    age_df = age_df.rename(columns={
        "playerid": "playerId",
        "playername": "playerName",
        "teamid": "teamId",
        "seasonkey": "seasonKey"
    })

    # keep only needed columns
    keep_cols = [c for c in ["playerId", "playerName", "age"] if c in age_df.columns]
    age_df = age_df[keep_cols].copy()

    # first pass: merge by playerId
    merged = p0.merge(
        age_df[["playerId", "age"]].drop_duplicates("playerId"),
        on="playerId",
        how="left"
    )

    # fallback pass for any missing ages: name-based
    missing_mask = merged["age"].isna()
    if missing_mask.any():
        p0_missing = merged.loc[missing_mask, ["playerId", "playerName"]].copy()
        p0_missing["nameKey"] = p0_missing["playerName"].apply(norm_name)

        age_name = age_df.copy()
        age_name["nameKey"] = age_name["playerName"].apply(norm_name)
        age_name = age_name.drop_duplicates("nameKey")

        fix = p0_missing.merge(age_name[["nameKey", "age"]], on="nameKey", how="left")
        merged.loc[missing_mask, "age"] = fix["age"].values

    # make age numeric
    merged["age"] = pd.to_numeric(merged["age"], errors="coerce")

    # optional: keep integer-like ages as Int64 (nullable)
    merged["age"] = merged["age"].round().astype("Int64")

    merged.to_csv(OUT_PATH, index=False)

    total = len(merged)
    matched = merged["age"].notna().sum()
    print(f"✅ wrote {OUT_PATH}")
    print(f"Age coverage: {matched}/{total}")

    if matched < total:
        print("\nPlayers still missing age:")
        print(merged.loc[merged["age"].isna(), ["playerId", "playerName", "teamId"]].to_string(index=False))


if __name__ == "__main__":
    main()