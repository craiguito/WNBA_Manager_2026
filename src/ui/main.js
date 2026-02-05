// LEGACY: will be removed after deep sim refactor
import { GameEngine } from '../sim/deep/GameEngine.js';
import { EventBus } from '../core/event_bus.js';
import { initGameData } from '../data/game_data.js';
import { init3DScene, updateBallPosition, clearPlayers, addPlayerToScene } from './scene_setup.js';

let engine = null;
let gameInterval = null;
let gameSpeed = 2;
let isPaused = false;
let lastTickTime = 0;
let allPlayersData = [];

// DOM Elements
const speedSlider = document.getElementById('speed-slider');
const speedLabel = document.getElementById('speed-label');
const setupScreen = document.getElementById('setup-screen');
const startBtn = document.getElementById('start-btn');
const scoreText = document.getElementById('score-text');

// Initialize speed slider
if (speedSlider) {
    speedSlider.addEventListener('input', (e) => {
        gameSpeed = parseInt(e.target.value);
        speedLabel.innerText = `${gameSpeed}x`;
    });
}

// Load roster data
async function loadData() {
    try {
        const data = await initGameData();
        allPlayersData = data.legacy.players;
    } catch(e) { console.error("Data Load Failed", e); }
}
loadData();

// THE "START GAME" (SETUP) BUTTON LOGIC
document.getElementById('init-game-btn').onclick = () => {
    const homeCode = document.getElementById('user-team-select').value;
    const awayCode = document.getElementById('cpu-team-select').value;
    
    // Hide the black setup screen
    setupScreen.style.display = 'none';

    // Prepare rosters
    const homePlayers = allPlayersData.filter(p => p.Team === homeCode).sort((a,b) => b.ovr - a.ovr);
    const awayPlayers = allPlayersData.filter(p => p.Team === awayCode).sort((a,b) => b.ovr - a.ovr);

    const eventBus = new EventBus();
    eventBus.on('PLAY_EVENT', (payload) => {
        if (payload?.message) {
            console.log(`[PLAY_EVENT] ${payload.message}`);
        }
    });

    engine = new GameEngine(
        { name: homeCode, code: homeCode, isUser: true, roster: homePlayers.slice(0, 5), bench: homePlayers.slice(5) },
        { name: awayCode, code: awayCode, isUser: false, roster: awayPlayers.slice(0, 5), bench: awayPlayers.slice(5) },
        { eventBus }
    );

    engine.onLog = (time, msg, data) => {
        const p = document.createElement('p');
        const mins = Math.floor(time / 60);
        const secs = (time % 60).toString().padStart(2, '0');
        p.innerText = `[${mins}:${secs}] ${msg}`;
        document.getElementById('commentary-box').prepend(p);
        
        // Move ball based on engine data
        if (data && data.z !== undefined) {
            const mult = engine.possessionSide === 'away' ? -1 : 1;
            updateBallPosition(data.z * mult, data.x * mult);
        }
    };

    init3DScene('court-container');
    spawn3DPlayers();
    startBtn.style.display = 'block'; // Make "Tip Off" button appear
    updateScoreboard();
};

function startGameLoop() {
    startBtn.style.display = 'none';
    lastTickTime = performance.now();

    function loop(timestamp) {
        if (!isPaused) {
            const msPerGameSecond = 1000 / gameSpeed;
            if (timestamp - lastTickTime >= msPerGameSecond) {
                const result = engine.tick();
                updateScoreboard();
                
                if (result === 'END_Q') {
                    cancelAnimationFrame(gameInterval);
                    showLockerRoom();
                    return;
                }
                lastTickTime = timestamp;
            }
        }
        gameInterval = requestAnimationFrame(loop);
    }
    gameInterval = requestAnimationFrame(loop);
}

// Link the "Tip Off" button to the loop
startBtn.onclick = startGameLoop;

function updateScoreboard() {
    const time = engine.timeRemaining;
    const mins = Math.floor(time / 60);
    const secs = (time % 60).toString().padStart(2, '0');
    scoreText.innerText = `${engine.homeTeam.code} ${engine.score.home} - ${engine.score.away} ${engine.awayTeam.code} (${mins}:${secs})`;
}

function spawn3DPlayers() {
    clearPlayers();
    engine.homeTeam.roster.forEach((p, i) => addPlayerToScene(p.Player, p.Pos, p.height_in/72, 1, 'home', i));
    engine.awayTeam.roster.forEach((p, i) => addPlayerToScene(p.Player, p.Pos, p.height_in/72, 1, 'away', i));
}

function showLockerRoom() {
    const locker = document.getElementById('locker-room');
    if (!locker) return;
    // Populate simple lists for starters/bench (home team only for now)
    const listStarters = document.getElementById('list-starters');
    const listBench = document.getElementById('list-bench');
    if (listStarters) {
        listStarters.innerHTML = '';
        engine.homeTeam.roster.forEach(p => {
            const d = document.createElement('div');
            d.style.padding = '6px';
            d.style.borderBottom = '1px solid #333';
            const nrg = p.currentStamina !== undefined ? p.currentStamina : (engine.stats[p.Player] && engine.stats[p.Player].EN) || 100;
            d.innerText = `${p.Player} — ${p.Pos} — NRG:${nrg}`;
            listStarters.appendChild(d);
        });
    }
    if (listBench) {
        listBench.innerHTML = '';
        (engine.homeTeam.bench || []).forEach(p => {
            const d = document.createElement('div');
            d.style.padding = '6px';
            d.style.borderBottom = '1px solid #333';
            const nrg = p.currentStamina !== undefined ? p.currentStamina : (engine.stats[p.Player] && engine.stats[p.Player].EN) || 100;
            d.innerText = `${p.Player} — ${p.Pos} — NRG:${nrg}`;
            listBench.appendChild(d);
        });
    }

    // Show locker room overlay
    locker.style.display = 'flex';

    const nextBtn = document.getElementById('next-qtr-btn');
    if (nextBtn) {
        nextBtn.onclick = () => {
            locker.style.display = 'none';
            // Reset timer for next quarter and resume loop
            engine.timeRemaining = 600;
            startGameLoop();
        };
    }
}
