import { loadAllData } from './loaders.js';
import { toLegacyPlayer } from './adapters/player_adapter.js';
import { buildTeamRosters } from './adapters/roster_adapter.js';
import { validateAll } from './validate_data.js';

export async function initGameData() {
    const raw = await loadAllData();

    validateAll({
        players: raw.players,
        teams: raw.teams,
        schedule: raw.schedule,
        coaches: raw.coaches,
        badges: raw.badges
    });

    const indices = {
        playersById: Object.fromEntries(raw.players.map((player) => [player.playerId, player])),
        teamsById: Object.fromEntries(raw.teams.map((team) => [team.teamId, team])),
        coachesById: Object.fromEntries(raw.coaches.map((coach) => [coach.coachId, coach])),
        badgesById: Object.fromEntries(raw.badges.map((badge) => [badge.badgeId, badge]))
    };

    const rostersByTeam = buildTeamRosters(raw.teams, raw.players);

    const legacyPlayers = raw.players.map((player) => toLegacyPlayer(player, indices.badgesById));

    return {
        raw,
        indices: { ...indices, rostersByTeam },
        legacy: { players: legacyPlayers }
    };
}
