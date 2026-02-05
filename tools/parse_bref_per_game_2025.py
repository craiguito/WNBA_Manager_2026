import argparse
import csv
import re
from pathlib import Path

from bs4 import BeautifulSoup


def normalize_header(text: str) -> str:
    if text is None:
        return ""
    t = text.strip().lower()
    t = t.replace("%", "pct").replace("/", "per")
    t = "".join(ch for ch in t if ch.isalnum())
    return t


def slugify(text: str) -> str:
    t = text.strip().lower()
    t = re.sub(r"[^a-z0-9]+", "_", t)
    t = t.strip("_")
    return t or "unknown"


def parse_float(value: str):
    if value is None:
        return None
    v = value.strip()
    if v in ("", "\u2014", "-", "\u2013"):
        return None
    try:
        return float(v)
    except ValueError:
        return None


def format_float(value, decimals: int = 3) -> str:
    if value is None:
        return ""
    return f"{value:.{decimals}f}"


HEADER_MAP = {
    "player": "player",
    "playername": "player",
    "tm": "team",
    "team": "team",
    "pos": "pos",
    "position": "pos",
    "age": "age",
    "g": "g",
    "gs": "gs",
    "mp": "mp",
    "mpg": "mpg",
    "mpperg": "mpg",
    "fg": "fg",
    "fga": "fga",
    "fgpct": "fgpct",
    "3p": "fg3",
    "3pa": "fg3a",
    "3ppct": "fg3pct",
    "2p": "fg2",
    "2pa": "fg2a",
    "2ppct": "fg2pct",
    "ft": "ft",
    "fta": "fta",
    "ftpct": "ftpct",
    "orb": "orb",
    "trb": "trb",
    "ast": "ast",
    "stl": "stl",
    "blk": "blk",
    "tov": "tov",
    "pf": "pf",
    "pts": "pts",
}


PHASE0_COLUMNS = ["playerId", "playerName", "teamId", "pos", "age"]
PHASE1_COLUMNS = ["playerId", "g", "mpg", "starterFlag", "usageProxyPer36"]
PHASE2_SHOOTING_COLUMNS = [
    "playerId",
    "fg",
    "fga",
    "fgPct",
    "fg3",
    "fg3a",
    "fg3Pct",
    "fg2",
    "fg2a",
    "fg2Pct",
    "ft",
    "fta",
    "ftPct",
    "pts",
]
PHASE2_BOX_COLUMNS = ["playerId", "orb", "trb", "ast", "stl", "blk", "tov", "pf"]


def load_html(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def extract_rows(html: str):
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if table is None:
        raise RuntimeError("No <table> found in HTML.")

    headers = []
    thead = table.find("thead")
    if thead:
        header_row = thead.find("tr")
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]

    if not headers:
        first_row = table.find("tr")
        if first_row:
            headers = [th.get_text(strip=True) for th in first_row.find_all(["th", "td"])]

    if not headers:
        raise RuntimeError("No headers found in table.")

    header_keys = [HEADER_MAP.get(normalize_header(h)) for h in headers]

    body = table.find("tbody")
    rows = body.find_all("tr") if body else table.find_all("tr")

    extracted = []
    for tr in rows:
        cells = tr.find_all(["th", "td"])
        if not cells:
            continue
        values = [cell.get_text(" ", strip=True) for cell in cells]
        row = {}
        for key, value in zip(header_keys, values):
            if key:
                row[key] = value

        player = row.get("player", "").strip()
        team = row.get("team", "").strip()
        if not player or player.lower() == "player":
            continue
        if not team or team.lower() == "team":
            continue
        extracted.append(row)

    return extracted, header_keys


def build_player_id(player_name: str, team: str) -> str:
    return f"bref_{team.lower()}_{slugify(player_name)}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse WNBA per-game table HTML to CSV outputs")
    parser.add_argument(
        "--input",
        default="raw_data/bref_wnba_2025_per_game_table.html.txt",
        help="Path to saved per-game HTML table",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        alt_path = Path("raw_data/bref_wnba_2025_per_game_table.html")
        if alt_path.exists():
            input_path = alt_path
        else:
            raise FileNotFoundError(f"Input file not found: {args.input}")

    html = load_html(input_path)
    rows, header_keys = extract_rows(html)

    deduped = []
    seen = set()
    for row in rows:
        player = row.get("player", "").strip()
        team = row.get("team", "").strip().upper()
        key = (player.lower(), team)
        if key in seen:
            continue
        seen.add(key)
        row["team"] = team
        deduped.append(row)

    raw_dir = Path("raw_data")
    raw_dir.mkdir(parents=True, exist_ok=True)

    has_age = "age" in header_keys

    phase0_path = raw_dir / "phase0_players_index_2025.csv"
    phase1_path = raw_dir / "phase1_players_workload_2025.csv"
    phase2_shooting_path = raw_dir / "phase2_players_shooting_2025.csv"
    phase2_box_path = raw_dir / "phase2_players_box_2025.csv"

    phase0_cols = ["playerId", "playerName", "teamId", "pos"] + (["age"] if has_age else [])

    with phase0_path.open("w", newline="", encoding="utf-8") as f0, \
        phase1_path.open("w", newline="", encoding="utf-8") as f1, \
        phase2_shooting_path.open("w", newline="", encoding="utf-8") as f2, \
        phase2_box_path.open("w", newline="", encoding="utf-8") as f3:

        w0 = csv.DictWriter(f0, fieldnames=phase0_cols)
        w1 = csv.DictWriter(f1, fieldnames=PHASE1_COLUMNS)
        w2 = csv.DictWriter(f2, fieldnames=PHASE2_SHOOTING_COLUMNS)
        w3 = csv.DictWriter(f3, fieldnames=PHASE2_BOX_COLUMNS)

        w0.writeheader()
        w1.writeheader()
        w2.writeheader()
        w3.writeheader()

        for row in deduped:
            player = row.get("player", "").strip()
            team = row.get("team", "").strip().upper()
            player_id = build_player_id(player, team)

            g = parse_float(row.get("g"))
            gs = parse_float(row.get("gs"))
            mp = parse_float(row.get("mp"))
            mpg_raw = row.get("mpg")
            mpg_val = parse_float(mpg_raw)

            if mpg_val is None and mp is not None and g:
                mpg_val = mp / g if g else None

            mp_total = mp
            if mp_total is None and mpg_val is not None and g is not None:
                mp_total = mpg_val * g

            starter_flag = ""
            if gs is not None and g:
                if g > 0 and (gs / g) >= 0.5:
                    starter_flag = "1"

            fga = parse_float(row.get("fga"))
            fta = parse_float(row.get("fta"))
            tov = parse_float(row.get("tov"))

            usage_proxy = ""
            if mp_total and mp_total > 0 and fga is not None and fta is not None and tov is not None:
                usage_proxy_val = (fga + 0.44 * fta + tov) * (36 / mp_total)
                usage_proxy = format_float(usage_proxy_val, 3)

            phase0_row = {
                "playerId": player_id,
                "playerName": player,
                "teamId": team,
                "pos": row.get("pos", ""),
            }
            if has_age:
                phase0_row["age"] = row.get("age", "")

            phase1_row = {
                "playerId": player_id,
                "g": row.get("g", ""),
                "mpg": mpg_raw if mpg_raw and mpg_raw.strip() else format_float(mpg_val, 3) if mpg_val is not None else "",
                "starterFlag": starter_flag,
                "usageProxyPer36": usage_proxy,
            }

            phase2_shooting_row = {
                "playerId": player_id,
                "fg": row.get("fg", ""),
                "fga": row.get("fga", ""),
                "fgPct": row.get("fgpct", ""),
                "fg3": row.get("fg3", ""),
                "fg3a": row.get("fg3a", ""),
                "fg3Pct": row.get("fg3pct", ""),
                "fg2": row.get("fg2", ""),
                "fg2a": row.get("fg2a", ""),
                "fg2Pct": row.get("fg2pct", ""),
                "ft": row.get("ft", ""),
                "fta": row.get("fta", ""),
                "ftPct": row.get("ftpct", ""),
                "pts": row.get("pts", ""),
            }

            phase2_box_row = {
                "playerId": player_id,
                "orb": row.get("orb", ""),
                "trb": row.get("trb", ""),
                "ast": row.get("ast", ""),
                "stl": row.get("stl", ""),
                "blk": row.get("blk", ""),
                "tov": row.get("tov", ""),
                "pf": row.get("pf", ""),
            }

            w0.writerow(phase0_row)
            w1.writerow(phase1_row)
            w2.writerow(phase2_shooting_row)
            w3.writerow(phase2_box_row)

    print(f"Rows read: {len(rows)}")
    print(f"Rows written: {len(deduped)}")
    print(f"Wrote {phase0_path.name}: {len(deduped)}")
    print(f"Wrote {phase1_path.name}: {len(deduped)}")
    print(f"Wrote {phase2_shooting_path.name}: {len(deduped)}")
    print(f"Wrote {phase2_box_path.name}: {len(deduped)}")


if __name__ == "__main__":
    main()

