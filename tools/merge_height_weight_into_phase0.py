import json
import re
import unicodedata
from pathlib import Path
import pandas as pd

PHASE0_IN = Path("raw_data/phase0_players_index_2025.csv")
PLAYERS_JSON = Path("data/players_with_badges.json")
PHASE0_OUT = Path("raw_data/phase0_players_index_2025_with_bio.csv")


def norm_name(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main():
    p0 = pd.read_csv(PHASE0_IN)

    with open(PLAYERS_JSON, "r", encoding="utf-8") as f:
        arr = json.load(f)

    # your json is a top-level list of objects with Player/height_in/weight_lb
    jdf = pd.DataFrame(arr)

    # keep only needed fields + normalize names
    required = ["Player", "height_in", "weight_lb"]
    missing_cols = [c for c in required if c not in jdf.columns]
    if missing_cols:
        raise RuntimeError(f"Missing columns in players_with_badges.json: {missing_cols}")

    jdf = jdf[required].copy()
    jdf["nameKey"] = jdf["Player"].map(norm_name)
    jdf.rename(columns={"height_in": "heightIn", "weight_lb": "weightLb"}, inplace=True)

    # dedupe in case same name appears more than once
    jdf = jdf.drop_duplicates(subset=["nameKey"], keep="first")

    p0["nameKey"] = p0["playerName"].map(norm_name)

    merged = p0.merge(
        jdf[["nameKey", "heightIn", "weightLb"]],
        on="nameKey",
        how="left"
    )

    # safety check
    if len(merged) != len(p0):
        raise RuntimeError(f"Row count changed after merge: {len(p0)} -> {len(merged)}")

    merged.drop(columns=["nameKey"], inplace=True)
    merged.to_csv(PHASE0_OUT, index=False)

    total = len(merged)
    h_ok = merged["heightIn"].notna().sum()
    w_ok = merged["weightLb"].notna().sum()

    print(f"✅ Wrote: {PHASE0_OUT}")
    print(f"Coverage: height {h_ok}/{total}, weight {w_ok}/{total}")

    miss = merged[merged["heightIn"].isna() | merged["weightLb"].isna()][["playerName", "teamId", "pos"]]
    if not miss.empty:
        print("\n⚠ Missing height/weight (first 25):")
        print(miss.head(25).to_string(index=False))


if __name__ == "__main__":
    main()