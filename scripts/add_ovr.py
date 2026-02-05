import json
import os

# 1. SETUP PATHS
script_dir = os.path.dirname(os.path.abspath(__file__))
input_path = os.path.join(script_dir, '..', 'data', 'players_with_badges.json')

# 2. LOAD DATA
try:
    with open(input_path, 'r') as f:
        players = json.load(f)
        print(f"Loaded {len(players)} players.")
except FileNotFoundError:
    print(f"Error: Could not find {input_path}")
    exit()

# 3. CALCULATE OVR
for p in players:
    # We take the average of the 6 core stats
    total_stats = (
        p.get('attr_Finishing', 0) +
        p.get('attr_Shooting', 0) +
        p.get('attr_Defense', 0) +
        p.get('attr_Rebounding', 0) +
        p.get('attr_Playmaking', 0) +
        p.get('attr_Stamina', 0)
    )
    
    # Round to nearest whole number
    p['ovr'] = int(round(total_stats / 6))

# 4. SAVE
with open(input_path, 'w') as f:
    json.dump(players, f, indent=2)

print("Success! OVR added to all players.")

# 5. PREVIEW TOP 5 PLAYERS
print("\n--- TOP 5 PLAYERS BY OVR ---")
players.sort(key=lambda x: x['ovr'], reverse=True)
for p in players[:5]:
    print(f"{p['ovr']} OVR - {p['Player']} ({p['Team']})")