import time
import random
import requests
import argparse
from pathlib import Path
import pandas as pd

SESSION = requests.Session()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.basketball-reference.com/wnba/",
}

def fetch(url: str, sleep_s: float = 2.0, retries: int = 4, verbose: bool = False) -> str:
    last_err = None
    last_status = None
    last_snippet = None
    for attempt in range(retries):
        try:
            r = SESSION.get(url, headers=HEADERS, timeout=30)
            last_status = r.status_code
            last_snippet = (r.text or "")[:200].replace("\n", " ").strip()
            if verbose:
                print(f"[fetch] attempt={attempt+1}/{retries} status={r.status_code}")
            if r.status_code == 403:
                # backoff before retry
                time.sleep(sleep_s + attempt * 2 + random.random())
                continue
            r.raise_for_status()
            time.sleep(sleep_s + random.random())  # polite jitter
            return r.text
        except Exception as e:
            last_err = e
            time.sleep(sleep_s + attempt * 2 + random.random())
    if last_err is None:
        details = f"status={last_status}" if last_status is not None else "no status"
        if last_snippet:
            details += f", body_start={last_snippet!r}"
        raise RuntimeError(f"Failed to fetch {url} after {retries} retries ({details})")
    raise last_err


def build_season_url(season: int, page: str) -> str:
    # Basketball-Reference WNBA season pages
    if page == "per_game":
        return f"https://www.basketball-reference.com/wnba/years/{season}_per_game.html"
    if page == "totals":
        return f"https://www.basketball-reference.com/wnba/years/{season}_totals.html"
    return f"https://www.basketball-reference.com/wnba/years/{season}.html"


def save_raw_html(season: int, html: str, page: str) -> Path:
    raw_dir = Path("raw_data")
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_path = raw_dir / f"bref_wnba_{season}_{page}.html"
    out_path.write_text(html, encoding="utf-8")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Scrape WNBA season index from Basketball-Reference")
    parser.add_argument("--season", type=int, required=True, help="Season year, e.g. 2025")
    parser.add_argument("--page", choices=["index", "per_game", "totals"], default="per_game",
                        help="Which page to fetch (default: per_game)")
    parser.add_argument("--sleep", type=float, default=2.0, help="Base sleep seconds between requests")
    parser.add_argument("--retries", type=int, default=4, help="Number of retries on failure")
    parser.add_argument("--verbose", action="store_true", help="Print fetch status per attempt")
    args = parser.parse_args()

    url = build_season_url(args.season, args.page)
    try:
        html = fetch(url, sleep_s=args.sleep, retries=args.retries, verbose=args.verbose)
        out_path = save_raw_html(args.season, html, args.page)
        print(f"Saved {args.season} {args.page} HTML to {out_path}")
        return
    except Exception as e:
        print(f"Primary fetch failed ({e}). Trying pandas.read_html fallback...")

    # Fallback: use pandas.read_html (as used in scripts/scraper.py)
    dfs = pd.read_html(url)
    if not dfs:
        raise RuntimeError(f"pandas.read_html returned no tables for {url}")
    table_html = dfs[0].to_html(index=False)
    out_path = save_raw_html(args.season, table_html, f"{args.page}_table")
    print(f"Saved {args.season} {args.page} table HTML to {out_path}")


if __name__ == "__main__":
    main()
