import { DayStateMachine, DayStates } from './day_state_machine.js';

export class SimManager {
    constructor({ deepSim = null, liteSim = null } = {}) {
        this.dayStateMachine = new DayStateMachine();
        this.deepSim = deepSim;
        this.liteSim = liteSim;
    }

    getDayState() {
        return this.dayStateMachine.getState();
    }

    advanceDayState() {
        return this.dayStateMachine.advance();
    }

    resetDay() {
        this.dayStateMachine.reset();
    }

    tick() {
        const state = this.getDayState();
        switch (state) {
            case DayStates.DAY_HUB:
            case DayStates.PRE_SIM_TO_USER_GAME:
            case DayStates.POST_SIM_REMAINDER:
                return this.runLitePhase(state);
            case DayStates.USER_GAME_LIVE:
                return this.runDeepPhase();
            case DayStates.DAY_COMPLETE:
            default:
                return null;
        }
    }

    runDeepPhase() {
        if (!this.deepSim || typeof this.deepSim.tick !== 'function') {
            return null;
        }
        return this.deepSim.tick();
    }

    runLitePhase(state) {
        if (!this.liteSim || typeof this.liteSim.tick !== 'function') {
            return null;
        }
        return this.liteSim.tick(state);
    }
}
