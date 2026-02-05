export class EventBus {
    constructor() {
        this.handlers = new Map();
    }

    on(eventName, handler) {
        if (!this.handlers.has(eventName)) {
            this.handlers.set(eventName, new Set());
        }
        this.handlers.get(eventName).add(handler);
    }

    off(eventName, handler) {
        const handlers = this.handlers.get(eventName);
        if (handlers) {
            handlers.delete(handler);
        }
    }

    emit(eventName, payload) {
        const handlers = this.handlers.get(eventName);
        if (!handlers) return;
        handlers.forEach((handler) => handler(payload));
    }
}
