# tools/wehoop_download_shot_locations_2025.R
# Downloads WNBA 2025 player shot location splits (league-wide) and writes CSVs.

suppressPackageStartupMessages({
  library(wehoop)
  library(dplyr)
  library(readr)
  library(stringr)
})

SEASON <- 2025
SEASON_TYPE <- "Regular Season"

OUT_DIR <- "raw_data"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

out_csv_main <- file.path(OUT_DIR, sprintf("phase3_shot_locations_%d.csv", SEASON))
out_csv_zone_summary <- file.path(OUT_DIR, sprintf("phase3_shot_zones_%d.csv", SEASON))

# helper: pick the first dataframe-like object from a wehoop list response
pick_df <- function(x) {
  if (is.data.frame(x)) return(x)
  if (is.list(x)) {
    dfs <- x[sapply(x, is.data.frame)]
    if (length(dfs) >= 1) return(dfs[[1]])
  }
  stop("Could not find a dataframe in the response.")
}

message("Fetching league-wide player shot locations...")

# This endpoint returns *shot location splits* (not every shot event)
resp <- wnba_leaguedashplayershotlocations(
  season = SEASON,
  season_type = SEASON_TYPE
)

df <- pick_df(resp)

# Normalize column names
names(df) <- names(df) |>
  str_replace_all("\\s+", "_") |>
  str_replace_all("[^A-Za-z0-9_]", "") |>
  tolower()

# Write the raw table so you can inspect exactly what fields came back
write_csv(df, out_csv_main)
message(sprintf("✅ wrote %s (%d rows, %d cols)", out_csv_main, nrow(df), ncol(df)))

# ---- build a clean "zone summary" table for your sim ----
# The shot locations table from stats APIs typically includes fields like:
# - player_id, player_name, team_id, team_abbreviation
# - shot_zone_basic, shot_zone_area, shot_zone_range
# - fgm, fga, fg_pct
# But naming varies slightly, so we handle common variants.

get_col <- function(df, candidates) {
  for (c in candidates) if (c %in% names(df)) return(c)
  NA_character_
}

player_id_col <- get_col(df, c("player_id", "playerid"))
player_name_col <- get_col(df, c("player_name", "playername", "player"))
team_id_col <- get_col(df, c("team_id", "teamid"))
team_abbr_col <- get_col(df, c("team_abbreviation", "teamabbr", "team"))
zone_basic_col <- get_col(df, c("shot_zone_basic", "shotzonebasic"))
zone_area_col  <- get_col(df, c("shot_zone_area", "shotzonearea"))
zone_range_col <- get_col(df, c("shot_zone_range", "shotzonerange"))
fgm_col <- get_col(df, c("fgm"))
fga_col <- get_col(df, c("fga"))

if (is.na(player_name_col) || is.na(fgm_col) || is.na(fga_col)) {
  message("⚠️ could not build zone summary because expected columns weren't found.")
  message("   inspect the raw output CSV and adjust mapping if needed.")
  quit(status = 0)
}

zone_df <- df %>%
  mutate(
    zone_basic = if (!is.na(zone_basic_col)) .data[[zone_basic_col]] else NA_character_,
    zone_area  = if (!is.na(zone_area_col))  .data[[zone_area_col]]  else NA_character_,
    zone_range = if (!is.na(zone_range_col)) .data[[zone_range_col]] else NA_character_,
    player_id  = if (!is.na(player_id_col))  .data[[player_id_col]]  else NA,
    player_name = .data[[player_name_col]],
    team_id    = if (!is.na(team_id_col))    .data[[team_id_col]]    else NA,
    team_abbr  = if (!is.na(team_abbr_col))  .data[[team_abbr_col]]  else NA_character_,
    fgm = .data[[fgm_col]],
    fga = .data[[fga_col]]
  ) %>%
  mutate(
    # build a single "zoneKey" you can map to your engine buckets
    zoneKey = paste(
      ifelse(is.na(zone_basic), "", zone_basic),
      ifelse(is.na(zone_area), "", zone_area),
      ifelse(is.na(zone_range), "", zone_range),
      sep = "|"
    )
  ) %>%
  group_by(player_id, player_name, team_abbr, zoneKey) %>%
  summarise(
    fgm = sum(fgm, na.rm = TRUE),
    fga = sum(fga, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    fg_pct = ifelse(fga > 0, fgm / fga, NA_real_)
  )

write_csv(zone_df, out_csv_zone_summary)
message(sprintf("✅ wrote %s (%d rows)", out_csv_zone_summary, nrow(zone_df)))

message("done.")