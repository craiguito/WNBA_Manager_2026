export class GameEngine {
    constructor(homeTeam, awayTeam) {
        this.homeTeam = this.initRoster(homeTeam);
        this.awayTeam = this.initRoster(awayTeam);
        this.score = { home: 0, away: 0 };
        this.timeRemaining = 600; // 10 Minutes
        
        this.currentState = 'START_GAME'; 
        this.stateTimer = 0; 
        this.possessionSide = 'home'; 
        this.currentHandler = null; 
        
        this.stats = {};
        [...this.homeTeam.fullRoster, ...this.awayTeam.fullRoster].forEach(p => {
            this.stats[p.Player] = { PTS: 0, FGM: 0, FGA: 0, "3PM": 0, "3PA": 0, AST: 0, EN: 100, PF: 0 };
        });
    }

    initRoster(team) {
        return {
            name: team.name,
            code: team.code, // Transfer team code for the UI
            isUser: team.isUser,
            roster: team.roster.map(p => ({ ...p, currentStamina: 100 })),
            bench: team.bench ? team.bench.map(p => ({ ...p, currentStamina: 100 })) : [],
            fullRoster: [...team.roster, ...(team.bench || [])].map(p => ({ ...p, currentStamina: 100 }))
        };
    }

    tick() {
        if (this.timeRemaining <= 0) return 'END_Q'; // Prevents negative time

        this.timeRemaining--;
        if (this.stateTimer > 0) {
            this.stateTimer--;
            return null;
        }
        return this.runStateLogic();
    }

    runStateLogic() {
        const offense = this.possessionSide === 'home' ? this.homeTeam : this.awayTeam;
        const defense = this.possessionSide === 'home' ? this.awayTeam : this.homeTeam;

        switch (this.currentState) {
            case 'START_GAME':
                this.currentState = 'POSSESSION_START';
                this.stateTimer = 1;
                return null;

            case 'POSSESSION_START':
                this.currentHandler = this.getWeightedPlayer(offense.roster);
                this.log(`> Inbounds to ${this.currentHandler.Player}`, {x: 0, z: -40});
                this.currentState = 'SHOOT';
                this.stateTimer = 4;
                return null;

            case 'SHOOT':
                const made = Math.random() > 0.6;
                if (made) {
                    this.score[this.possessionSide] += 2;
                    this.log(`> ${this.currentHandler.Player} scores! (+2)`, {x: 0, z: 40});
                    this.possessionSide = (this.possessionSide === 'home' ? 'away' : 'home');
                } else {
                    this.log(`> ${this.currentHandler.Player} misses.`, {x: 0, z: 40});
                }
                this.currentState = 'POSSESSION_START';
                this.stateTimer = 2;
                return null;
        }
    }

    // FIXED: Restored missing logic
    getWeightedPlayer(roster) {
        const totalWeight = roster.reduce((sum, player) => sum + (player.ovr) * (0.2 + player.currentStamina / 150), 0);
        let randomVal = Math.random() * totalWeight;
        for (const player of roster) {
            const weight = (player.ovr) * (0.2 + player.currentStamina / 150);
            if (randomVal < weight) return player;
            randomVal -= weight;
        }
        return roster[0];
    }

    log(message, data) {
        if (this.onLog) this.onLog(this.timeRemaining, message, data);
    }
}