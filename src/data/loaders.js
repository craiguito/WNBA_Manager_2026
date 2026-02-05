const cache = new Map();

export async function loadJSON(path) {
    if (cache.has(path)) {
        return cache.get(path);
    }

    const loadPromise = (async () => {
        if (typeof window === 'undefined') {
            const { readFile } = await import('node:fs/promises');
            const { resolve } = await import('node:path');
            const resolvedPath = path.startsWith('/')
                ? resolve(process.cwd(), path.slice(1))
                : resolve(process.cwd(), path);
            const contents = await readFile(resolvedPath, 'utf-8');
            return JSON.parse(contents);
        }

        const response = await fetch(path);
        if (!response.ok) {
            throw new Error(`Failed to load ${path}: ${response.status}`);
        }
        return response.json();
    })();

    cache.set(path, loadPromise);
    return loadPromise;
}

export async function loadAllData() {
    const [players, teams, coaches, badges, schedule, tendencies] = await Promise.all([
        loadJSON('/data/players.json'),
        loadJSON('/data/teams.json'),
        loadJSON('/data/coaches.json'),
        loadJSON('/data/badges.json'),
        loadJSON('/data/schedule.json'),
        loadJSON('/data/tendencies.json')
    ]);

    return { players, teams, coaches, badges, schedule, tendencies };
}
