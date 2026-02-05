// LEGACY: will be removed after deep sim refactor
export function toLegacyPlayer(player, badgesIndex = {}) {
    const fullName = player.displayName || `${player.firstName} ${player.lastName}`.trim();
    const attributes = player.attributes || {};
    const rebounding = attributes.rebounding ?? Math.round(((attributes.defense ?? 50) + (attributes.finishing ?? 50)) / 2);

    const badgeNames = (player.badges || []).map((badgeId) => {
        const badge = badgesIndex[badgeId];
        return badge ? badge.name : badgeId;
    });

    return {
        Player: fullName,
        Team: player.teamId,
        Pos: player.position,
        height_in: player.heightIn,
        weight_lb: player.weightLb,
        attr_Finishing: attributes.finishing ?? 50,
        attr_Shooting: attributes.shooting ?? 50,
        attr_Defense: attributes.defense ?? 50,
        attr_Rebounding: rebounding,
        attr_Playmaking: attributes.playmaking ?? 50,
        attr_Stamina: attributes.stamina ?? 70,
        // Derived defaults to preserve legacy consumers that expect these fields.
        attr_Discipline: player.shootingProfile?.shotDiscipline ?? 60,
        attr_FreeThrow: player.shootingProfile?.midRange?.rating ?? 65,
        ovr: Math.round(((attributes.finishing ?? 50) + (attributes.shooting ?? 50) + (attributes.defense ?? 50) + (attributes.playmaking ?? 50)) / 4),
        badges: badgeNames
    };
}
