#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd


def load_maps(mapping_csv: Path):
    m = pd.read_csv(mapping_csv, low_memory=False)

    required = {"sr_player_uuid", "canonical_playerId", "sr_team_uuid", "teamId"}
    missing = required - set(m.columns)
    if missing:
        raise RuntimeError(f"Mapping CSV missing columns {missing}. Found: {m.columns.tolist()}")

    player_map = (
        m.dropna(subset=["sr_player_uuid", "canonical_playerId"])
         .assign(sr_player_uuid=lambda d: d["sr_player_uuid"].astype(str),
                 canonical_playerId=lambda d: d["canonical_playerId"].astype(str))
         .set_index("sr_player_uuid")["canonical_playerId"]
         .to_dict()
    )

    team_map = (
        m.dropna(subset=["sr_team_uuid", "teamId"])
         .assign(sr_team_uuid=lambda d: d["sr_team_uuid"].astype(str),
                 teamId=lambda d: d["teamId"].astype(str))
         .groupby("sr_team_uuid")["teamId"]
         .agg(lambda s: s.value_counts().index[0])
         .to_dict()
    )

    return player_map, team_map


def replace_ids(df: pd.DataFrame, col: str, mp: dict[str, str], issues: list[dict], file_tag: str, kind: str):
    if col not in df.columns:
        return df
    s = df[col].astype("string")
    mapped = s.map(mp)

    # keep unmapped as original (so you don’t lose info), but record issue
    out = s.where(mapped.isna(), mapped)
    df[col] = out

    bad = s.dropna()
    bad = bad[~bad.isin(mp.keys())]
    if len(bad) > 0:
        issues.append({
            "file": file_tag,
            "type": f"unmapped_{kind}",
            "column": col,
            "n_unique": int(bad.nunique()),
            "examples": ";".join(bad.unique().astype(str)[:10]),
        })
    return df


def process_one(in_path: Path, out_path: Path, player_map, team_map, issues: list[dict]):
    file_tag = in_path.name
    df = pd.read_csv(in_path, low_memory=False)

    if file_tag == "phase4_player_event_rates_2025.csv":
        df = replace_ids(df, "team_id", team_map, issues, file_tag, "team_uuid")
        df = replace_ids(df, "player_id", player_map, issues, file_tag, "player_uuid")
        # game_id stays SR uuid (fine as canonical for now)

    elif file_tag == "phase4_lineup_stints_2025.csv":
        df = replace_ids(df, "team_id", team_map, issues, file_tag, "team_uuid")
        for c in ["p1", "p2", "p3", "p4", "p5"]:
            df = replace_ids(df, c, player_map, issues, file_tag, "player_uuid")

    elif file_tag == "phase4_team_style_2025.csv":
        df = replace_ids(df, "team_id", team_map, issues, file_tag, "team_uuid")

    elif file_tag == "phase4_game_context_2025.csv":
        df = replace_ids(df, "home_team_id", team_map, issues, file_tag, "team_uuid")
        df = replace_ids(df, "away_team_id", team_map, issues, file_tag, "team_uuid")

    else:
        raise RuntimeError(f"Unsupported file: {file_tag}")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    issues.append({"file": file_tag, "type": "wrote", "out": str(out_path), "rows": int(len(df)), "cols": int(df.shape[1])})


def main():
    ap = argparse.ArgumentParser(description="Rekey phase4 core outputs (player/team UUIDs -> canonical ids).")
    ap.add_argument("--mapping", required=True, help="sr_uuid_to_phase0_playerid_2025.csv")
    ap.add_argument("--in-dir", required=True, help="Folder containing the 4 phase4 csv files")
    ap.add_argument("--out-dir", required=True, help="Folder to write *_canonical.csv")
    ap.add_argument("--issues-out", default="raw_data/maps/rekey_phase4_core_issues.csv")
    args = ap.parse_args()

    player_map, team_map = load_maps(Path(args.mapping))

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    targets = [
        "phase4_player_event_rates_2025.csv",
        "phase4_lineup_stints_2025.csv",
        "phase4_team_style_2025.csv",
        "phase4_game_context_2025.csv",
    ]

    issues: list[dict] = []

    for fname in targets:
        in_path = in_dir / fname
        if not in_path.exists():
            issues.append({"file": fname, "type": "missing_input"})
            continue
        out_path = out_dir / fname.replace(".csv", "_canonical.csv")
        process_one(in_path, out_path, player_map, team_map, issues)

    Path(args.issues_out).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(issues).to_csv(args.issues_out, index=False)
    print("done")
    print(f"out-dir: {out_dir}")
    print(f"issues: {args.issues_out}")


if __name__ == "__main__":
    main()
