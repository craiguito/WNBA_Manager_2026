# Copilot / AI Agent Instructions — WNBA_Manager_2026

Purpose: Give AI coding agents immediate context and actionable rules for working in this repo.

- **Big picture:** This project is a small WNBA team manager/prototype. Core pieces:
  - Simulation runner: [run_sim.js](run_sim.js) — top-level Node entry to run simulations.
  - Game engine: [src/GameEngine.js](src/GameEngine.js) — simulation logic, player state updates, and game rules.
  - UI/scene: [main.js](main.js) and [scene_setup.js](scene_setup.js) — front-end setup (index.html) and scene wiring.
  - Data sources: `data/` contains `players.json` and `players_with_badges.json` — canonical player records used by the sim.
  - Scripts: `scripts/` contains Python helpers: `add_ovr.py` (compute/assign overall ratings), `assign_badges.py` (badge logic), `scraper.py` (data collection).
  - Docs: [docs/architecture.md](docs/architecture.md) — high-level design notes. Read before changing major flows.

- **Why things are structured this way:** Simulation is separated from UI. `run_sim.js` invokes the engine directly so automated runs and headless testing are simple (Node), while `main.js`/`scene_setup.js` orchestrate the browser UI.

- **Developer workflows / commands**
  - Run a headless simulation: `node run_sim.js` (run from repo root). If errors reference missing Python-generated data, run the relevant script in `scripts/`.
  - Refresh player OVRs/badges: `python scripts/add_ovr.py` then `python scripts/assign_badges.py` (Python 3 required).
  - Scrape/source new players: `python scripts/scraper.py` — outputs to `data/`.

- **Project-specific conventions**
  - Player data is stored as JSON in `data/` and expected to contain specific keys used by `src/GameEngine.js`. When adding fields, update both the Python scripts and `src/GameEngine.js` usage.
  - Badge state lives in `data/players_with_badges.json` — `assign_badges.py` is the authoritative generator.
  - Keep heavy logic in `src/GameEngine.js`. UI files (`main.js`, `scene_setup.js`) should only handle rendering and event wiring.

- **Integration points & external deps**
  - Node.js (for `run_sim.js`, `src/` modules). Use Node v16+ if possible.
  - Python 3 for scripts in `scripts/`. These scripts read/write `data/*.json` files consumed by Node code.
  - No package manager files present — changes that add packages should include a `package.json` or `requirements.txt`.

- **Patterns to follow when modifying code**
  - When changing simulation data shapes, update both the data generator scripts (`scripts/*.py`) and the engine (`src/GameEngine.js`). Example: adding a `stamina` stat requires Python script output and engine usage adjustments.
  - Minimal surface changes in UI: add new rendering hooks in `scene_setup.js` and keep simulation logic in `GameEngine.js`.
  - Preserve existing JSON keys; if you need to migrate, write a small migration script in `scripts/` and update `docs/architecture.md`.

- **Examples of concise prompts an AI agent can follow**
  - "Add a `stamina` field to players: update `scripts/add_ovr.py` to emit `stamina`, update `data/players.json` generation, and read/use `stamina` in `src/GameEngine.js` during player fatigue calculations." 
  - "Refactor game loop in `src/GameEngine.js` to extract `applyPlayerFatigue()` — keep external behavior identical and add unit-friendly hooks." 

- **When merging existing copilot instructions**
  - If `.github/copilot-instructions.md` already exists, preserve any repo-specific rules and examples. Append or replace only outdated top-level workflow commands (e.g., Python paths or Node run commands) after validating current files.

If any behaviors, scripts, or external tools are missing from this file, tell me what to add or share the missing commands/files and I'll update this instruction doc.
