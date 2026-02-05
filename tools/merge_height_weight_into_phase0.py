import json
import re
import unicodedata
from pathlib import Path

import pandas as pd

PHASE0_IN = Path("raw_data/phase0_players_index_2025.csv")
PLAYERS_JSON = Path("data/players_with_badges.json")
PHASE0_OUT = Path("raw_data/phase0_players_index_2025_with_bio.csv")


def norm_name(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s'\-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_height_to_inches(v):
    """
    Handles:
      - 74, 74.0
      - "74"
      - "6'2"
      - "6-2"
      - "6 2"
      - returns int or pd.NA
    """
    if v is None:
        return pd.NA
    if isinstance(v, (int, float)) and not pd.isna(v):
        return int(round(float(v)))

    s = str(v).strip().lower()
    if s in ("", "nan", "none", "null"):
        return pd.NA

    # plain numeric string
    if re.fullmatch(r"\d+(\.\d+)?", s):
        return int(round(float(s)))

    # feet-inches patterns
    m = re.search(r"(\d+)\s*['\- ]\s*(\d+)", s)
    if m:
        ft = int(m.group(1))
        inch = int(m.group(2))
        return ft * 12 + inch

    return pd.NA


def parse_weight_lb(v):
    """
    Handles:
      - 165, 165.0
      - "165"
      - "165 lbs", "165lb"
      - returns float or pd.NA
    """
    if v is None:
        return pd.NA
    if isinstance(v, (int, float)) and not pd.isna(v):
        return float(v)

    s = str(v).strip().lower()
    if s in ("", "nan", "none", "null"):
        return pd.NA

    s = re.sub(r"[^0-9.]", "", s)
    if s == "":
        return pd.NA
    return float(s)


def pick_field(d: dict, candidates):
    for c in candidates:
        if c in d:
            return d[c]
    return None


def main():
    if not PHASE0_IN.exists():
        raise FileNotFoundError(f"Missing {PHASE0_IN}")
    if not PLAYERS_JSON.exists():
        raise FileNotFoundError(f"Missing {PLAYERS_JSON}")

    p0 = pd.read_csv(PHASE0_IN)
    with open(PLAYERS_JSON, "r", encoding="utf-8") as f:
        arr = json.load(f)

    rows = []
    for p in arr:
        name = pick_field(p, ["playerName", "name", "displayName"])
        h = pick_field(p, ["heightIn", "height", "height_in"])
        w = pick_field(p, ["weightLb", "weight", "weight_lb"])

        if name is None:
            continue

        rows.append({
            "playerName_json": str(name).strip(),
            "nameKey": norm_name(name),
            "heightIn": parse_height_to_inches(h),
            "weightLb": parse_weight_lb(w),
            "bioScore": int(pd.notna(parse_height_to_inches(h))) + int(pd.notna(parse_weight_lb(w)))
        })

    jdf = pd.DataFrame(rows)
    if jdf.empty:
        raise RuntimeError("No player rows parsed from players_with_badges.json")

    # keep best row per normalized name (prefer row with both height+weight)
    jdf = (
        jdf.sort_values(["bioScore"], ascending=False)
           .drop_duplicates(subset=["nameKey"], keep="first")
    )

    p0["nameKey"] = p0["playerName"].apply(norm_name)

    merged = p0.merge(
        jdf[["nameKey", "heightIn", "weightLb"]],
        on="nameKey",
        how="left",
        validate="one_to_one"
    )

    merged = merged.drop(columns=["nameKey"])
    merged.to_csv(PHASE0_OUT, index=False, encoding="utf-8")

    total = len(merged)
    h_ok = merged["heightIn"].notna().sum()
    w_ok = merged["weightLb"].notna().sum()

    print(f"✅ Wrote: {PHASE0_OUT}")
    print(f"Coverage: height {h_ok}/{total}, weight {w_ok}/{total}")

    missing = merged[merged["heightIn"].isna() | merged["weightLb"].isna()][["playerName", "teamId", "pos"]]
    if not missing.empty:
        print("\n⚠ Missing height or weight (first 20):")
        print(missing.head(20).to_string(index=False))


if __name__ == "__main__":
    main()