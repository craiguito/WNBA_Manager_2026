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
    if v is None:
        return pd.NA
    if isinstance(v, (int, float)) and not pd.isna(v):
        return int(round(float(v)))
    s = str(v).strip().lower()
    if s in ("", "nan", "none", "null"):
        return pd.NA
    if re.fullmatch(r"\d+(\.\d+)?", s):
        return int(round(float(s)))
    m = re.search(r"(\d+)\s*['\- ]\s*(\d+)", s)
    if m:
        return int(m.group(1)) * 12 + int(m.group(2))
    return pd.NA


def parse_weight_lb(v):
    if v is None:
        return pd.NA
    if isinstance(v, (int, float)) and not pd.isna(v):
        return float(v)
    s = str(v).strip().lower()
    if s in ("", "nan", "none", "null"):
        return pd.NA
    s = re.sub(r"[^0-9.]", "", s)
    if not s:
        return pd.NA
    return float(s)


def deep_get(d, path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def pick_field(d: dict, candidates):
    for c in candidates:
        if isinstance(c, tuple):
            v = deep_get(d, c, None)
            if v is not None:
                return v
        else:
            if c in d and d[c] is not None:
                return d[c]
    return None


def extract_player_array(obj):
    # case 1: top-level list
    if isinstance(obj, list):
        return obj

    # case 2: top-level dict with common keys
    if isinstance(obj, dict):
        for k in ["players", "data", "items", "roster", "results"]:
            v = obj.get(k)
            if isinstance(v, list):
                return v
        # if dict values contain exactly one list-like candidate
        list_values = [v for v in obj.values() if isinstance(v, list)]
        if len(list_values) == 1:
            return list_values[0]

    return []


def main():
    if not PHASE0_IN.exists():
        raise FileNotFoundError(f"Missing {PHASE0_IN}")
    if not PLAYERS_JSON.exists():
        raise FileNotFoundError(f"Missing {PLAYERS_JSON}")

    p0 = pd.read_csv(PHASE0_IN)

    with open(PLAYERS_JSON, "r", encoding="utf-8") as f:
        obj = json.load(f)

    arr = extract_player_array(obj)
    if not arr:
        raise RuntimeError(
            "Could not find player list in players_with_badges.json. "
            "Expected top-level list or dict containing players/data/items/roster."
        )

    rows = []
    for p in arr:
        if not isinstance(p, dict):
            continue

        name = pick_field(p, [
            "playerName", "name", "fullName", "displayName",
            ("bio", "name"), ("player", "name")
        ])
        h = pick_field(p, [
            "heightIn", "height", "height_in",
            ("bio", "height"), ("measurements", "height")
        ])
        w = pick_field(p, [
            "weightLb", "weight", "weight_lb",
            ("bio", "weight"), ("measurements", "weight")
        ])

        if name is None:
            continue

        h_in = parse_height_to_inches(h)
        w_lb = parse_weight_lb(w)

        rows.append({
            "playerName_json": str(name).strip(),
            "nameKey": norm_name(name),
            "heightIn": h_in,
            "weightLb": w_lb,
            "bioScore": int(pd.notna(h_in)) + int(pd.notna(w_lb))
        })

    jdf = pd.DataFrame(rows)
    if jdf.empty:
        raise RuntimeError("No player rows parsed from players_with_badges.json")

    # keep best row per name
    jdf = (
        jdf.sort_values(["bioScore"], ascending=False)
           .drop_duplicates(subset=["nameKey"], keep="first")
    )

    p0["nameKey"] = p0["playerName"].apply(norm_name)

    merged = p0.merge(
        jdf[["nameKey", "heightIn", "weightLb"]],
        on="nameKey",
        how="left"
    )

    # guard: ensure no duplicate phase0 rows created
    if len(merged) != len(p0):
        raise RuntimeError(
            f"Merge changed row count: phase0={len(p0)} merged={len(merged)}. "
            "Check duplicate names in source JSON."
        )

    merged.drop(columns=["nameKey"], inplace=True)
    merged.to_csv(PHASE0_OUT, index=False, encoding="utf-8")

    total = len(merged)
    h_ok = merged["heightIn"].notna().sum()
    w_ok = merged["weightLb"].notna().sum()

    print(f"✅ Wrote: {PHASE0_OUT}")
    print(f"Coverage: height {h_ok}/{total}, weight {w_ok}/{total}")

    missing = merged[merged["heightIn"].isna() | merged["weightLb"].isna()][["playerName", "teamId", "pos"]]
    if not missing.empty:
        print("\n⚠ Missing height or weight (first 25):")
        print(missing.head(25).to_string(index=False))


if __name__ == "__main__":
    main()