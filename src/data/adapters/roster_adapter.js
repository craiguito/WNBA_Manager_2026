export function buildTeamRosters(teams, players) {
    const rosters = {};
    const playerIds = new Set(players.map((player) => player.playerId));

    teams.forEach((team) => {
        rosters[team.teamId] = Array.isArray(team.roster) ? [...team.roster] : [];
        rosters[team.teamId].forEach((playerId) => {
            if (!playerIds.has(playerId)) {
                console.warn(`Roster reference missing playerId: ${playerId} for team ${team.teamId}`);
            }
        });
    });

    return rosters;
}
