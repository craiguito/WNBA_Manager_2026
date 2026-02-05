import pandas as pd
import unicodedata
import re
from pathlib import Path

PHASE0 = Path("raw_data/phase0_players_index_2025.csv")
PHASE3_IN = Path("raw_data/phase3_shot_locations_2025.csv")
PHASE3_OUT = Path("raw_data/phase3_player_shot_profile_2025.csv")


def norm_name(s):
    if pd.isna(s):
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s'-]", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def main():
    # load data
    p0 = pd.read_csv(PHASE0)
    s3 = pd.read_csv(PHASE3_IN)

    # normalize column names
    s3.columns = (
        s3.columns
        .str.lower()
        .str.replace(r"[^\w]+", "_", regex=True)
        .str.strip("_")
    )

    # normalize names for join
    p0["nameKey"] = p0["playerName"].apply(norm_name)
    s3["nameKey"] = s3["player_name"].apply(norm_name)

    # zone calculations
    df = s3.copy()

    df["rim_fgm"] = df["restricted_area_fgm"]
    df["rim_fga"] = df["restricted_area_fga"]

    df["paint_fgm"] = df["paint_non_ra_fgm"]
    df["paint_fga"] = df["paint_non_ra_fga"]

    df["mid_fgm"] = df["mid_range_fgm"]
    df["mid_fga"] = df["mid_range_fga"]

    df["corner3_fgm"] = df["corner_3_fgm"]
    df["corner3_fga"] = df["corner_3_fga"]

    df["ab3_fgm"] = df["above_break_3_fgm"]
    df["ab3_fga"] = df["above_break_3_fga"]

    df["three_fgm"] = df["corner3_fgm"] + df["ab3_fgm"]
    df["three_fga"] = df["corner3_fga"] + df["ab3_fga"]

    df["total_fgm"] = (
        df["rim_fgm"]
        + df["paint_fgm"]
        + df["mid_fgm"]
        + df["three_fgm"]
    )
    df["total_fga"] = (
        df["rim_fga"]
        + df["paint_fga"]
        + df["mid_fga"]
        + df["three_fga"]
    )

    # efficiencies
    for z in ["rim", "paint", "mid", "corner3", "ab3", "three", "total"]:
        df[f"{z}_fg"] = df[f"{z}_fgm"] / df[f"{z}_fga"].replace({0: pd.NA})

    # tendencies (attempt share)
    for z in ["rim", "paint", "mid", "corner3", "ab3", "three"]:
        df[f"{z}_att_share"] = df[f"{z}_fga"] / df["total_fga"].replace({0: pd.NA})

    # merge to phase0
    merged = p0.merge(
        df,
        on="nameKey",
        how="left",
        validate="one_to_one"
    )

    keep = [
        "playerId", "playerName", "teamId", "pos",
        "rim_fgm","rim_fga","rim_fg",
        "paint_fgm","paint_fga","paint_fg",
        "mid_fgm","mid_fga","mid_fg",
        "corner3_fgm","corner3_fga","corner3_fg",
        "ab3_fgm","ab3_fga","ab3_fg",
        "three_fgm","three_fga","three_fg",
        "total_fgm","total_fga","total_fg",
        "rim_att_share","paint_att_share","mid_att_share",
        "corner3_att_share","ab3_att_share","three_att_share"
    ]

    merged[keep].to_csv(PHASE3_OUT, index=False)

    print(f"✅ wrote {PHASE3_OUT}")
    print(f"coverage: {merged['total_fga'].notna().sum()} / {len(merged)} players")


if __name__ == "__main__":
    main()