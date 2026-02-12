# Current State Audit (Data/Backend Focus)

_Date: 2026-02-07_

## Scope
This audit intentionally focuses on **data pipeline + backend asset readiness** and excludes simulation engine/UI implementation details.

## Where the project is right now

### 1) You have a strong 2025 data pipeline foundation
The repo already has a multi-phase ingestion + transformation path that appears to be moving toward a canonical player feature model:

- `raw_data/phase0...phase4` contains staged source/intermediate data.
- `raw_data/phase4_canonical/` contains large canonicalized play-by-play derivatives.
- `derived/player_feature_mart_2025.csv` exists as a consolidated analytical table.
- `tools/build_player_feature_mart.py` is a clean join script for phase0..phase4 into one mart.

This is the most valuable part of the repo for your Unity/C# direction.

### 2) You currently have two incompatible player-data “worlds”
- **Legacy game-json world:** `data/players_with_badges.json` + old scripts in `scripts/`.
- **New structured model world:** `data/players_test.json` generated from phases by `tools/build_players_test_from_phases.py`.

These two schemas are different, and both are still present/used by different code paths.

### 3) Some top-level game metadata is placeholder-scale
`data/teams.json`, `data/schedule.json`, and parts of `data/players.json` are tiny/sample-sized compared with full player datasets.

This suggests that league-level runtime data is not productionized yet and should be regenerated from canonical sources before Unity integration.

## Keep vs Deprecate

## Keep (high priority)

1. **`raw_data/phase*_2025*.csv` chain (especially canonical phase4 outputs)**
   - This is the most expensive work to recreate and should be preserved as historical source-of-truth input snapshots.

2. **`derived/player_feature_mart_2025.csv` and related derived tables**
   - Good intermediate contract for analytics, balancing, and eventual Unity import preprocessing.

3. **`tools/` scripts tied to phase processing and canonicalization**
   - Especially:
     - `build_player_feature_mart.py`
     - `build_players_test_from_phases.py`
     - rekey/merge scripts for phase alignment

4. **ID mapping artifacts under `raw_data/maps/`**
   - These mappings prevent painful joins/mismatches later; keep versioned.

## Deprecate / archive candidate

1. **`scripts/` legacy pipeline (`scraper.py`, `assign_badges.py`, `add_ovr.py`)**
   - These rely on a much older single-table stat approach and produce legacy schema JSON.
   - Keep only for historical reference; stop using for forward development.

2. **`data/players_with_badges.json` as primary source**
   - Should no longer be authoritative if your future path is canonical phase data -> feature mart -> Unity export.

3. **Backup clutter in `raw_data` (`*.bak`, `*.bak2`)**
   - Move into an explicit `raw_data/archive/` or DVC/LFS-managed snapshot bucket.

4. **Case-inconsistent files (e.g., `raw_data/sr/2025-games.JSON`)**
   - Normalize naming (`.json`) to prevent cross-platform friction in tooling.

## What needs work next (Unity/C# oriented)

### Priority A — Define one authoritative export contract
Create a single schema contract for Unity import, e.g.:
- `exports/unity/players_2025.json`
- `exports/unity/teams_2025.json`
- `exports/unity/schedule_2026.json`

Action:
- Add one Python exporter script that reads `derived/player_feature_mart_2025.csv` (plus canonical team/schedule) and writes Unity-friendly JSON.
- Include stable IDs, enums, and value ranges (0-100 ratings, tendency floats, etc.).

### Priority B — Unify around canonical IDs
Standardize and enforce:
- `playerId`
- `teamId`
- `season_year`

Action:
- Add a validator script that fails fast on duplicates/null IDs in every export.

### Priority C — Separate source, intermediate, and deliverable datasets
Introduce explicit folder contracts:
- `raw_data/` = immutable source snapshots
- `derived/` = reproducible transformed tables
- `exports/` = runtime-ready game payloads for Unity

### Priority D — Add reproducibility entrypoint
Right now there is no single documented “build all data” command.

Action:
- Add `Makefile` or `python tools/build_all.py` to run key steps in order.
- Emit logs/checkpoints for each phase and row-count sanity checks.

## Proposed deprecation policy

1. Mark legacy artifacts with `_legacy` naming (or move to `legacy/`).
2. Freeze legacy scripts as read-only.
3. Announce cutover date in docs.
4. After one stable Unity export cycle, remove legacy runtime files from active paths.

## 30/60/90 day practical roadmap

### 0–30 days
- Pick and document the Unity data contract.
- Build exporter + validator.
- Stop consuming `data/players_with_badges.json` in new work.

### 31–60 days
- Generate full teams/schedule payloads from canonical data.
- Introduce CI check for data integrity (schema + IDs + non-null core fields).

### 61–90 days
- Archive/delete legacy scripts and sample-scale placeholder JSON files.
- Lock reproducible pipeline and version exports per season.

## Bottom line
You are in a **good data-engineering position** for a Unity pivot, but you need to complete the transition from a prototype JSON workflow to a **single canonical, reproducible export pipeline**.
