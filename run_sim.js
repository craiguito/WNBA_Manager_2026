import fs from 'fs';
import { GameEngine } from './src/sim/deep/GameEngine.js';

// 1. LOAD PLAYERS
const rawData = fs.readFileSync('./data/players_with_badges.json');
const allPlayers = JSON.parse(rawData);

// HELPER: Calculate OVR to sort starters
const getOVR = (p) => {
    return (p.attr_Finishing + p.attr_Shooting + p.attr_Defense + p.attr_Playmaking) / 4;
};

// 2. DRAFT TEAMS (Sorted by Best OVR)
const getStarters = (teamCode) => {
    return allPlayers
        .filter(p => p.Team === teamCode)
        .sort((a, b) => getOVR(b) - getOVR(a))
        .slice(0, 5); 
};

const aces = { name: "Las Vegas Aces", roster: getStarters('LVA') };
const fever = { name: "Indiana Fever", roster: getStarters('IND') };

console.log(`\nStarting Simulation: ${aces.name} vs ${fever.name}`);
console.log("------------------------------------------------");
console.log(`ACES STARTERS: ${aces.roster.map(p => p.Player).join(', ')}`);
console.log(`FEVER STARTERS: ${fever.roster.map(p => p.Player).join(', ')}`);
console.log("------------------------------------------------");

// 3. START ENGINE
const engine = new GameEngine(aces, fever);
engine.simulateQuarter();

// 4. PRINT THE BOX SCORE (This was missing!)
engine.printBoxScore();

console.log("------------------------------------------------");
console.log("Simulation Complete.");
