import pandas as pd
import numpy as np
import json
import os
import time

# 1. SETUP
script_dir = os.path.dirname(os.path.abspath(__file__))
output_path = os.path.join(script_dir, '..', 'data', 'players_with_badges.json')

years = [2022, 2023, 2024, 2025]
weights = {2025: 4.0, 2024: 3.0, 2023: 2.0, 2022: 1.0}
data_frames = {}

# 2. SCRAPE LOOP
for year in years:
    url = f"https://www.basketball-reference.com/wnba/years/{year}_per_game.html"
    print(f"Connecting to {year} WNBA Database...")
    try:
        dfs = pd.read_html(url)
        df = dfs[0]
        df = df[df['Player'] != 'Player']
        
        # NUMERIC CLEANING
        cols = ['G', 'MP', 'FG%', '3P', '3P%', 'TRB', 'AST', 'STL', 'BLK', 'PTS', 'PF', 'FT%']
        for col in cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
        
        if 'Pos' in df.columns:
            df['Pos'] = df['Pos'].astype(str).replace('nan', np.nan)
        
        df = df.sort_values('G', ascending=False).drop_duplicates('Player')
        df.set_index('Player', inplace=True)
        data_frames[year] = df
        time.sleep(2) 
    except Exception as e:
        print(f"Error scraping {year}: {e}")

# 3. MERGE & BACKFILL
print("Filtering for Active 2025 Roster...")
if 2025 not in data_frames: exit()

# STRICTLY LOAD 2025 DATA ONLY
# We do NOT concatenate missing players from previous years anymore.
final_df = data_frames[2025].copy()

# === POSITION FIX ===
if 'Pos' not in final_df.columns:
    final_df['Pos'] = np.nan

# We still LOOK at old years to fill gaps for CURRENT players,
# but we do not add new players.
for year in [2024, 2023, 2022]:
    if year in data_frames and 'Pos' in data_frames[year].columns:
        final_df['Pos'] = final_df['Pos'].fillna(data_frames[year]['Pos'])

final_df['Pos'] = final_df['Pos'].fillna('G')
final_df['Pos'] = final_df['Pos'].apply(lambda x: str(x).replace('-', '/'))
# ====================

stat_cols = ['PTS', 'AST', 'TRB', 'STL', 'BLK', '3P', '3P%', 'FG%', 'MP', 'PF', 'FT%']
for col in stat_cols:
    final_df[col] = final_df[col].astype(float)

# Blend stats (Weighted Average)
for player in final_df.index:
    for col in stat_cols:
        numerator = 0.0
        denominator = 0.0
        for year in years:
            if year in data_frames and player in data_frames[year].index:
                val = data_frames[year].loc[player, col]
                w = weights[year]
                numerator += (val * w)
                denominator += w
        if denominator > 0:
            final_df.loc[player, col] = numerator / denominator

# 4. PHYSICALS & ATTRIBUTES
final_df['height_in'] = final_df['TRB'].apply(lambda x: 70 + int(x) if x < 10 else 78) 
if 'Wt' in final_df.columns:
    final_df['weight_lb'] = pd.to_numeric(final_df['Wt'], errors='coerce').fillna(170)
else:
    final_df['weight_lb'] = final_df['height_in'] * 2.1 + (final_df['TRB'] * 2)

print("Calculating Ratings...")
df = final_df 

def get_dominance_rating(stat_col):
    if stat_col not in df.columns: return 25
    league_max = df[stat_col].max()
    if league_max == 0: return 25
    relative_score = df[stat_col] / league_max
    curved_score = np.power(relative_score, 0.7) 
    return (25 + (curved_score * 74)).astype(int)

df['attr_Finishing'] = get_dominance_rating('PTS')
vol_score = get_dominance_rating('3P')
eff_score = (25 + (df['3P%'].rank(pct=True) * 74))
df['attr_Shooting'] = (vol_score * 0.8) + (eff_score * 0.2)
df['attr_Shooting'] = df['attr_Shooting'].astype(int)

df['attr_Playmaking'] = get_dominance_rating('AST')
df['DefenseScore'] = df['STL'] + df['BLK']
df['attr_Defense'] = get_dominance_rating('DefenseScore')
df['attr_Rebounding'] = get_dominance_rating('TRB')
df['attr_Stamina'] = get_dominance_rating('MP')
df['attr_Discipline'] = (25 + (df['PF'].rank(ascending=False, pct=True) * 74)).astype(int)
df['attr_FreeThrow'] = (df['FT%'] * 100).fillna(65).astype(int)

df['ovr'] = ((df['attr_Finishing'] + df['attr_Shooting'] + df['attr_Defense'] + 
              df['attr_Rebounding'] + df['attr_Playmaking'] + df['attr_Stamina']) / 6).astype(int)

# EXPORT
df = df.sort_values('ovr', ascending=False)
df['Player'] = df.index 
# Reset badges to empty (Use assign_badges.py after this!)
df['badges'] = [[] for _ in range(len(df))]

roster = df[['Player', 'Team', 'Pos', 'height_in', 'weight_lb', 
             'attr_Finishing', 'attr_Shooting', 'attr_Defense', 
             'attr_Rebounding', 'attr_Playmaking', 'attr_Stamina',
             'attr_Discipline', 'attr_FreeThrow', 'ovr', 'badges']].to_dict(orient='records')

os.makedirs(os.path.dirname(output_path), exist_ok=True)
with open(output_path, 'w') as f:
    json.dump(roster, f, indent=2)

print(f"Success! {len(roster)} players processed (Strict 2025 Roster).")