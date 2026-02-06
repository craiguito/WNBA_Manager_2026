#!/usr/bin/env python3
"""
Fetch WNBA play-by-play for every game ID in 2025-games.JSON

- Auth: x-api-key header (preferred), with fallback to ?api_key=
- Retries: handles 429/5xx with exponential backoff
- Output: one JSON file per game + manifest CSV

Usage (PowerShell):
  $env:SPORTRADAR_API_KEY="YOUR_KEY_HERE"
  python tools/fetch_pbp_2025.py --games-file 2025-games.JSON --out-dir raw_data/pbp_2025

Optional:
  python tools/fetch_pbp_2025.py --games-file 2025-games.JSON --out-dir raw_data/pbp_2025 --overwrite
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple

import requests


def load_dotenv_if_present(dotenv_path: Path = Path(".env")) -> None:
    """Minimal .env loader so no extra dependency is required."""
    if not dotenv_path.exists():
        return
    for line in dotenv_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


def read_games(games_file: Path) -> List[Dict[str, Any]]:
    data = json.loads(games_file.read_text(encoding="utf-8"))
    games = data.get("games", [])
    if not isinstance(games, list):
        raise ValueError("Invalid games JSON: expected top-level 'games' list.")
    return games


def fetch_one_pbp(
    session: requests.Session,
    base_url: str,
    locale: str,
    game_id: str,
    api_key: str,
    max_retries: int = 5,
    timeout_s: int = 40,
) -> Tuple[str, int, Dict[str, Any] | None, str]:
    """
    Returns: (fetch_status, http_code, json_data, error_message)
    fetch_status in {"ok", "not_available", "error"}
    """
    url = f"{base_url.rstrip('/')}/{locale}/games/{game_id}/pbp.json"

    # Attempt 1: x-api-key header
    for attempt in range(max_retries):
        try:
            resp = session.get(
                url,
                headers={"x-api-key": api_key, "accept": "application/json"},
                timeout=timeout_s,
            )
            code = resp.status_code

            if code == 200:
                return "ok", code, resp.json(), ""
            if code in (404, 410, 422):
                return "not_available", code, None, f"HTTP {code}"
            if code in (429, 500, 502, 503, 504):
                sleep_s = min(20, 2 ** attempt)
                time.sleep(sleep_s)
                continue

            # auth fallback attempt with query string
            if code in (401, 403):
                qresp = session.get(
                    url,
                    params={"api_key": api_key},
                    headers={"accept": "application/json"},
                    timeout=timeout_s,
                )
                qcode = qresp.status_code
                if qcode == 200:
                    return "ok", qcode, qresp.json(), ""
                if qcode in (404, 410, 422):
                    return "not_available", qcode, None, f"HTTP {qcode}"
                if qcode in (429, 500, 502, 503, 504):
                    sleep_s = min(20, 2 ** attempt)
                    time.sleep(sleep_s)
                    continue
                return "error", qcode, None, qresp.text[:500]

            return "error", code, None, resp.text[:500]

        except requests.RequestException as e:
            sleep_s = min(20, 2 ** attempt)
            if attempt == max_retries - 1:
                return "error", -1, None, str(e)
            time.sleep(sleep_s)

    return "error", -1, None, "max retries exceeded"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--games-file", required=True, help="Path to 2025-games.JSON")
    parser.add_argument("--out-dir", required=True, help="Directory for per-game pbp json")
    parser.add_argument(
        "--base-url",
        default="https://api.sportradar.com/wnba/trial/v8",
        help="Base API URL (default uses https).",
    )
    parser.add_argument("--locale", default="en", help="Locale segment in URL, e.g. en")
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key (optional if SPORTRADAR_API_KEY env var is set)",
    )
    parser.add_argument(
        "--only-closed",
        action="store_true",
        help="Only fetch games with status=closed from games file",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--sleep", type=float, default=0.25, help="Sleep between successful calls")
    parser.add_argument("--max-retries", type=int, default=5)
    args = parser.parse_args()

    load_dotenv_if_present()

    api_key = args.api_key or os.getenv("SPORTRADAR_API_KEY")
    if not api_key:
        raise SystemExit(
            "No API key found. Set SPORTRADAR_API_KEY env var, .env file, or pass --api-key."
        )

    games_file = Path(args.games_file)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "pbp_fetch_manifest.csv"

    games = read_games(games_file)
    if args.only_closed:
        games = [g for g in games if str(g.get("status", "")).lower() == "closed"]

    rows: List[Dict[str, Any]] = []
    ok = 0
    not_available = 0
    errors = 0

    with requests.Session() as session:
        for i, g in enumerate(games, start=1):
            game_id = g.get("id")
            status = g.get("status")
            scheduled = g.get("scheduled")
            reference = g.get("reference")

            if not game_id:
                rows.append(
                    {
                        "game_id": "",
                        "reference": reference,
                        "scheduled": scheduled,
                        "game_status": status,
                        "fetch_status": "error",
                        "http_code": "",
                        "file_path": "",
                        "error": "missing game id",
                    }
                )
                errors += 1
                continue

            out_file = out_dir / f"{game_id}.json"
            if out_file.exists() and not args.overwrite:
                rows.append(
                    {
                        "game_id": game_id,
                        "reference": reference,
                        "scheduled": scheduled,
                        "game_status": status,
                        "fetch_status": "skipped_exists",
                        "http_code": "",
                        "file_path": str(out_file),
                        "error": "",
                    }
                )
                continue

            fetch_status, code, payload, err = fetch_one_pbp(
                session=session,
                base_url=args.base_url,
                locale=args.locale,
                game_id=game_id,
                api_key=api_key,
                max_retries=args.max_retries,
            )

            if fetch_status == "ok" and payload is not None:
                out_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
                ok += 1
            elif fetch_status == "not_available":
                not_available += 1
            else:
                errors += 1

            rows.append(
                {
                    "game_id": game_id,
                    "reference": reference,
                    "scheduled": scheduled,
                    "game_status": status,
                    "fetch_status": fetch_status,
                    "http_code": code,
                    "file_path": str(out_file) if fetch_status == "ok" else "",
                    "error": err,
                }
            )

            if args.sleep > 0:
                time.sleep(args.sleep)

            if i % 25 == 0:
                print(f"[{i}/{len(games)}] ok={ok} not_available={not_available} errors={errors}")

    # write manifest
    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "game_id",
                "reference",
                "scheduled",
                "game_status",
                "fetch_status",
                "http_code",
                "file_path",
                "error",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print("\nDone.")
    print(f"Games considered: {len(games)}")
    print(f"PBP downloaded : {ok}")
    print(f"Not available  : {not_available}")
    print(f"Errors         : {errors}")
    print(f"Manifest       : {manifest_path}")


if __name__ == "__main__":
    main()
