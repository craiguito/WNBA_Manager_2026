export const DayStates = Object.freeze({
    DAY_HUB: 'DAY_HUB',
    PRE_SIM_TO_USER_GAME: 'PRE_SIM_TO_USER_GAME',
    USER_GAME_LIVE: 'USER_GAME_LIVE',
    POST_SIM_REMAINDER: 'POST_SIM_REMAINDER',
    DAY_COMPLETE: 'DAY_COMPLETE'
});

const STATE_ORDER = [
    DayStates.DAY_HUB,
    DayStates.PRE_SIM_TO_USER_GAME,
    DayStates.USER_GAME_LIVE,
    DayStates.POST_SIM_REMAINDER,
    DayStates.DAY_COMPLETE
];

export class DayStateMachine {
    constructor(initialState = DayStates.DAY_HUB) {
        if (!STATE_ORDER.includes(initialState)) {
            throw new Error(`Invalid day state: ${initialState}`);
        }
        this.state = initialState;
    }

    getState() {
        return this.state;
    }

    canAdvance() {
        return this.state !== DayStates.DAY_COMPLETE;
    }

    advance() {
        if (!this.canAdvance()) {
            return this.state;
        }
        const index = STATE_ORDER.indexOf(this.state);
        this.state = STATE_ORDER[index + 1];
        return this.state;
    }

    reset() {
        this.state = DayStates.DAY_HUB;
    }
}
