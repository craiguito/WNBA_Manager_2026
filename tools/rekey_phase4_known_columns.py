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
        raise RuntimeError(f"Mapping CSV missing {missing}. Found columns: {m.columns.tolist()}")

    # player uuid -> canonical playerId
    player_map = (
        m.dropna(subset=["sr_player_uuid", "canonical_playerId"])
         .assign(sr_player_uuid=lambda d: d["sr_player_uuid"].astype(str),
                 canonical_playerId=lambda d: d["canonical_playerId"].astype(str))
         .set_index("sr_player_uuid")["canonical_playerId"]
         .to_dict()
    )

    # team uuid -> canonical teamId (alias)
    team_map = (
        m.dropna(subset=["sr_team_uuid", "teamId"])
         .assign(sr_team_uuid=lambda d: d["sr_team_uuid"].astype(str),
                 teamId=lambda d: d["teamId"].astype(str))
         .groupby("sr_team_uuid")["teamId"]
         .agg(lambda s: s.value_counts().index[0])
         .to_dict()
    )

    return player_map, team_map


def map_col(df: pd.DataFrame, col: str, mp: dict[str, str], issues: list[dict], kind: str, file_tag: str):
    if col not in df.columns:
        return df
    s = df[col].astype("string")
    # keep original
    df[f"{col}__sr"] = s

    mapped = s.map(mp)
    # keep original where not mapped (so you can see issues + not lose info)
    out = s.where(mapped.isna(), mapped)

    # issues
    bad = s.dropna()
    bad = bad[~bad.isin(mp.keys())]
    if len(bad) > 0:
        issues.append({
            "file": file_tag,
            "type": f"unmapped_{kind}",
            "column": col,
            "count": int(bad.nunique()),
            "examples": ";".join(bad.unique().astype(str)[:10]),
        })

    df[col] = out
    return df


def map_game_id(df: pd.DataFrame, col: str, issues: list[dict], file_tag: str, game_map: dict[str, str] | None = None):
    if col not in df.columns:
        return df
    s = df[col].astype("string")
    df[f"{col}__sr"] = s

    if game_map:
        mapped = s.map(game_map)
        out = s.where(mapped.isna(), mapped)
        bad = s.dropna()
        bad = bad[~bad.isin(game_map.keys())]
        if len(bad) > 0:
            issues.append({
                "file": file_tag,
                "type": "unmapped_game_id",
                "column": col,
                "count": int(bad.nunique()),
                "examples": ";".join(bad.unique().astype(str)[:10]),
            })
        df[col] = out
    else:
        # no canonical game map provided; keep sr uuid as game_id (but preserved in __sr)
        df[col] = s
    return df


def process_file(path: Path, out_dir: Path, player_map, team_map, issues, game_map=None):
    file_tag = path.name
    df = pd.read_csv(path, low_memory=False)

    # per-file deterministic rekey
    if file_tag == "pbp_events.csv":
        df = map_game_id(df, "game_id", issues, file_tag, game_map)
        for c in [
            "attribution_team_id",
            "possession_team_id",
        ]:
            df = map_col(df, c, team_map, issues, "team_uuid", file_tag)

    elif file_tag == "pbp_lineups.csv":
        df = map_game_id(df, "game_id", issues, file_tag, game_map)
        # event_id stays SR (it’s internal per game), don’t touch
        for c in ["lineup_team_id"]:
            df = map_col(df, c, team_map, issues, "team_uuid", file_tag)
        for c in ["player_id"]:
            df = map_col(df, c, player_map, issues, "player_uuid", file_tag)

    elif file_tag == "phase4_player_event_rates_2025.csv":
        df = map_game_id(df, "game_id", issues, file_tag, game_map)
        df = map_col(df, "team_id", team_map, issues, "team_uuid", file_tag)
        df = map_col(df, "player_id", player_map, issues, "player_uuid", file_tag)

    elif file_tag == "phase4_lineup_stints_2025.csv":
        df = map_game_id(df, "game_id", issues, file_tag, game_map)
        df = map_col(df, "team_id", team_map, issues, "team_uuid", file_tag)
        for c in ["p1", "p2", "p3", "p4", "p5"]:
            df = map_col(df, c, player_map, issues, "player_uuid", file_tag)

    elif file_tag == "phase4_team_style_2025.csv":
        df = map_col(df, "team_id", team_map, issues, "team_uuid", file_tag)

    elif file_tag == "phase4_game_context_2025.csv":
        df = map_game_id(df, "game_id", issues, file_tag, game_map)
        for c in ["home_team_id", "away_team_id"]:
            df = map_col(df, c, team_map, issues, "team_uuid", file_tag)

    else:
        # if you point it at a folder with extra csvs, leave them untouched
        issues.append({"file": file_tag, "type": "skipped_unknown_file"})
        return

    out_path = out_dir / f"{path.stem}_canonical.csv"
    df.to_csv(out_path, index=False)
    issues.append({"file": file_tag, "type": "wrote", "out": str(out_path), "rows": int(len(df)), "cols": int(df.shape[1])})


def main():
    ap = argparse.ArgumentParser(description="Rekey known phase4 + pbp tables from Sportradar UUIDs to canonical ids.")
    ap.add_argument("--mapping", required=True, help="raw_data/maps/sr_uuid_to_phase0_playerid_2025.csv")
    ap.add_argument("--in-dir", required=True, help="Folder containing pbp_events.csv, pbp_lineups.csv, phase4_*.csv")
    ap.add_argument("--out-dir", required=True, help="Output folder for *_canonical.csv")
    ap.add_argument("--issues-out", default="raw_data/maps/rekey_phase4_known_columns_issues.csv")
    # optional if you ever create a canonical game schedule map
    ap.add_argument("--game-map", default="", help="Optional CSV with columns sr_game_id, gameId (canonical)")
    args = ap.parse_args()

    player_map, team_map = load_maps(Path(args.mapping))

    game_map = None
    if args.game_map:
        gm = pd.read_csv(args.game_map, low_memory=False)
        if not {"sr_game_id", "gameId"}.issubset(set(gm.columns)):
            raise RuntimeError("game-map CSV must have columns: sr_game_id, gameId")
        game_map = (
            gm.dropna(subset=["sr_game_id", "gameId"])
              .assign(sr_game_id=lambda d: d["sr_game_id"].astype(str),
                      gameId=lambda d: d["gameId"].astype(str))
              .set_index("sr_game_id")["gameId"]
              .to_dict()
        )

    in_dir = Path(args.in_dir)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    targets = [
        "pbp_events.csv",
        "pbp_lineups.csv",
        "phase4_player_event_rates_2025.csv",
        "phase4_lineup_stints_2025.csv",
        "phase4_team_style_2025.csv",
        "phase4_game_context_2025.csv",
    ]

    issues: list[dict] = []

    for fname in targets:
        p = in_dir / fname
        if not p.exists():
            issues.append({"file": fname, "type": "missing_input"})
            continue
        process_file(p, out_dir, player_map, team_map, issues, game_map=game_map)

    pd.DataFrame(issues).to_csv(args.issues_out, index=False)
    print("done")
    print(f"outputs -> {out_dir}")
    print(f"issues -> {args.issues_out}")


if __name__ == "__main__":
    main()
