# WNBA Manager Game – Architecture Overview

## Purpose
This document defines the current and intended architecture of the WNBA Manager Game.

The goal of the project is to build a **deep, watchable basketball management and simulation game** that:
- feels like a real broadcast when watched
- rewards knowledge and decision-making
- does NOT rely on simple RNG-based outcomes
- separates simulation logic from presentation and UI
- scales cleanly as features are added

This document is the source of truth for architecture decisions.

---

## High-Level Design Philosophy

- **Outcomes are visible, mechanisms are hidden**
- **The engine simulates decisions, not dice**
- **One deep simulation at a time; everything else runs in lite mode**
- **Time outside games is turn-based (day-by-day), not real-time**
- **UI never drives logic — it only reflects state**

---

## Core Game Loop (Day-Based)

The game progresses one calendar day at a time.

### Day States
1. **DAY_HUB**
   - Time is frozen
   - Player manages team, roster, finances, staff, scouting
   - Player can view schedule, stats, league info

2. **PRE_SIM_TO_USER_GAME**
   - If other games start before the user’s scheduled game:
     - Those games are simulated in lite mode up to user tipoff
     - Their intermediate state is stored (score, period, clock)

3. **USER_GAME_LIVE**
   - One game runs in **deep simulation**
   - Other live games run in **shadow lite simulation**
   - Broadcast overlay + ticker are active

4. **POST_SIM_REMAINDER**
   - Remaining games are finished in lite mode
   - “Simulating other games…” screen shown

5. **DAY_COMPLETE**
   - Results finalized
   - Stats saved
   - Player advances to next day

---

## Simulation Fidelity Levels

### Deep Simulation (One Game Only)
Used for:
- User-controlled games
- Spectator watch mode

Includes:
- Player movement and spacing
- Offensive sets and defensive coverages
- Decision-making (shoot / pass / drive / reset)
- Passing lane logic
- Defensive pressure and help logic
- Fatigue, momentum, composure
- Broadcast events (runs, close games, milestones)

### Lite Simulation (Background Games)
Used for:
- Non-user games
- Pre-sim and post-sim phases

Includes:
- Chunk-based possession simulation
- Score progression
- Major moment detection (runs, lead changes)
- Final stat generation

No real-time movement or animation.

---

## Simulation Architecture Layers

### 1. Tactics Layer
Defines *what* a team is trying to do.
- Offensive sets (PnR, motion, horns, etc.)
- Defensive schemes (man, zone, drop, switch)
- Chosen by Coach AI based on personnel and situation

### 2. Decision Layer
Defines *choices* players make.
- Utility-based evaluation (shoot / drive / pass / reset)
- Perceived value ≠ true value
- Decisions influenced by:
  - Vision
  - IQ
  - Composure
  - Aggression
  - Risk tolerance
  - Fatigue and pressure

### 3. Movement Layer
Defines *how* players move.
- Target-based movement (anchor spots)
- Simple steering, spacing enforcement
- Defensive positioning (on-ball, help, recover)

---

## Player Data Model

### Public (Player-Facing)
- Box score stats
- Season averages
- Shot charts and zone maps
- Role, archetype, badges
- Height, weight, position

### Soft-Visible (Descriptive Only)
Shown as labels or bars:
- Fatigue (Fresh / Tiring / Gassed)
- Confidence (Cold / Neutral / Hot)
- Injury Risk (Low / Medium / High)
- Chemistry (Poor / Developing / Strong)
- Coach Trust (Low / Stable / High)

### Hidden (Engine-Only)
Never shown numerically:
- Vision
- BBIQ
- Composure
- Risk tolerance
- Aggression bias
- Processing speed
- Pass accuracy variance
- Defensive anticipation
- Momentum modifiers
- Relationship values

---

## Coaches & Staff

### Coaches
- Have skill levels and tendencies
- Influence:
  - Set selection
  - Defensive schemes
  - Rotation logic
  - Advice quality (can be wrong)
- Coach advice accuracy depends on coach ability

### Staff (Concessions, Janitorial, etc.)
- Affect:
  - Attendance
  - Fan sentiment
  - Revenue
  - Arena cleanliness
- Quality varies per hire

---

## League & Schedule System

- Schedule is fixed per season
- Each game has:
  - Date
  - Tipoff time
  - Home / Away teams
- Games are ordered by tipoff time
- Time only advances during simulations
- Outside of games, time is frozen

---

## Broadcast & Presentation Layer

### Broadcast Overlay
- Scorebug
- Shot clock
- Period / game clock
- Lower thirds
- Run indicators
- Replay wipes (future)

### Live Ticker
- Shows:
  - Live scores
  - Major moments
  - Finals
- Only displays games currently simulated during USER_GAME_LIVE

### Spectator Mode
- Any game can be watched
- No control unless Sandbox mode
- Coach AI controls both teams

---

## Difficulty System

Difficulty affects:
- Coach advice accuracy
- UI hints and warnings
- Ambiguity of descriptors
- Error forgiveness

Difficulty does NOT:
- Buff player ratings
- Force scripted outcomes

---

## Performance Principles

- Only one deep sim runs at a time
- All other games use lite sim
- Decision ticks run at fixed intervals (not per frame)
- Heavy computations are cached
- UI charts are computed on demand
- Object pooling is used to avoid GC spikes

---

## File Structure Intent

/docs
architecture.md
sim_design.md
stat_visibility.md

/src
/core → day flow, scheduling, state machines
/sim → deep + lite simulation logic
/ai → coach + player decision models
/data → static player/team/coach data
/ui → broadcast overlay, ticker, menus


---

## Current State vs Future Direction

### Current
- Browser-based prototype
- Probability-heavy legacy engine
- Minimal movement logic

### Direction
- Unity-based implementation
- Deep sim focused on decisions + movement
- Probability only used for execution variance
- Clear separation between sim, AI, and UI

---

## Non-Goals (Important)
- Not trying to replicate NBA2K-level animation systems
- Not exposing raw engine probabilities to players
- Not simulating every off-ball micro-action at full fidelity

---

## Guiding Rule
If a player can say:
> “I don’t know exactly why that happened, but it makes sense”

Then the architecture is doing its job.
