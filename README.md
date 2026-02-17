# WNBA Manager 2026

WNBA Manager 2026 is a simulation-focused basketball management prototype built around a **day-based league loop** and a **deep-vs-lite simulation model**.

The project combines:
- a browser-facing game shell (`index.html` + `src/ui`)
- a deep game engine prototype (`src/sim/deep/GameEngine.js`)
- data pipelines and transformation scripts (`tools/`, `scripts/`)
- generated datasets for player and lineup analytics (`derived/`, `raw_data/`)

## Project Goals

- Simulate basketball decisions (not just random outcomes)
- Keep game logic separated from UI/presentation
- Support one deeply simulated game at a time while other games run in lite mode
- Build toward a watchable management + broadcast experience

For architecture details, see [`docs/architecture.md`](docs/architecture.md).

## Repository Structure

- `src/core/` – day flow and simulation orchestration
- `src/sim/deep/` – deep simulation engine components
- `src/ui/` – browser-side setup and main scene logic
- `data/` – core runtime JSON data (players, teams, schedule, badges, tendencies)
- `tools/` – ETL/build scripts for ingesting and deriving basketball datasets
- `scripts/` – utility scripts for enrichment and scraping
- `raw_data/` – source/intermediate data files
- `derived/` – generated analytics and feature tables
- `docs/` – architecture and design documentation

## Quick Start

### 1) Run a sample deep simulation in Node

```bash
node run_sim.js
```

This runs a sample matchup (Aces vs Fever) using `players_with_badges.json` and prints a simple box score summary.

### 2) Open the browser prototype

Serve the repo root with any static file server, then open `index.html` in your browser.

Example (Python):

```bash
python -m http.server 8000
```

Then browse to:

```text
http://localhost:8000
```

## Data + Pipeline Notes

- Most heavy data processing workflows live in `tools/`.
- Generated outputs are written into `derived/`.
- Runtime game data currently lives in `data/` JSON files.

If you are extending data ingestion or feature engineering, start with `tools/README_tools.md`.

## Current Status

This repository appears to be an active prototype with simulation and data tooling evolving in parallel. Expect iteration on data schemas and engine behavior as systems are integrated.
