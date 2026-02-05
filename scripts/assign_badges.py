import json
import os

# 1. SETUP
script_dir = os.path.dirname(os.path.abspath(__file__))
data_path = os.path.join(script_dir, '..', 'data', 'players_with_badges.json')

# 2. LOAD DATA
try:
    with open(data_path, 'r') as f:
        players = json.load(f)
    print(f"Loaded {len(players)} players.")
except FileNotFoundError:
    print("Error: players_with_badges.json not found. Run the scraper first.")
    exit()

# 3. DEFINE BADGE LOGIC
def calculate_badges(p):
    badges = []
    
    # ATTRIBUTES (Safe .get defaults to 0 if missing)
    finishing = p.get('attr_Finishing', 0)
    shooting = p.get('attr_Shooting', 0)
    defense = p.get('attr_Defense', 0)
    rebounding = p.get('attr_Rebounding', 0)
    playmaking = p.get('attr_Playmaking', 0)
    stamina = p.get('attr_Stamina', 0)
    
    # 1. SNIPER (Elite Shooting)
    if shooting >= 90:
        badges.append("Sniper")
        
    # 2. LOCKDOWN (Elite Defense)
    if defense >= 85:
        badges.append("Lockdown")
        
    # 3. FLOOR GENERAL (Elite Playmaking)
    if playmaking >= 90:
        badges.append("Floor General")
        
    # 4. GLASS CLEANER (Elite Rebounding)
    if rebounding >= 90:
        badges.append("Glass Cleaner")
        
    # 5. POST POWERHOUSE (Elite Inside Scoring)
    if finishing >= 95:
        badges.append("Post Powerhouse")
        
    # 6. WORKHORSE (High Stamina + Defense)
    if stamina >= 90 and defense >= 75:
        badges.append("Workhorse")
        
    # 7. OFFENSIVE ENGINE (Great All-Around Scorer)
    if finishing >= 80 and shooting >= 80 and playmaking >= 80:
        badges.append("Offensive Engine")
        
    # 8. THE ERASER (Blocks)
    # We check if they are a Center/Forward with high defense
    # (Since we don't track BLK attribute directly, we use Defense + Height as a proxy)
    if defense >= 88 and p.get('height_in', 0) >= 76:
        badges.append("The Eraser")

    return badges

# 4. APPLY BADGES (Safe Update)
for player in players:
    # We simply update the 'badges' key. We DO NOT create a new dictionary.
    # This ensures 'Pos', 'attr_Discipline', etc. are preserved.
    player['badges'] = calculate_badges(player)

# 5. SAVE
with open(data_path, 'w') as f:
    json.dump(players, f, indent=2)

print(f"Success! Badges assigned to {len(players)} players (Data Preserved).")