export function validateAll({ players, teams, schedule, coaches, badges }) {
    const teamIds = new Set(teams.map((team) => team.teamId));
    const playerIds = new Set(players.map((player) => player.playerId));

    players.forEach((player) => {
        if (player.teamId && !teamIds.has(player.teamId)) {
            console.warn(`Player ${player.playerId} references unknown teamId ${player.teamId}`);
        }
    });

    teams.forEach((team) => {
        if (Array.isArray(team.roster)) {
            team.roster.forEach((playerId) => {
                if (!playerIds.has(playerId)) {
                    console.warn(`Team ${team.teamId} roster missing playerId ${playerId}`);
                }
            });
        }
    });

    schedule.forEach((game) => {
        if (!teamIds.has(game.homeTeamId)) {
            console.warn(`Schedule game ${game.gameId} references unknown home team ${game.homeTeamId}`);
        }
        if (!teamIds.has(game.awayTeamId)) {
            console.warn(`Schedule game ${game.gameId} references unknown away team ${game.awayTeamId}`);
        }
    });

    if (!coaches.length) {
        console.warn('No coaches found in data set.');
    }

    if (!badges.length) {
        console.warn('No badges found in data set.');
    }
}
