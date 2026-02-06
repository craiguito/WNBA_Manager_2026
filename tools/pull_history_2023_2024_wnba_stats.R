# tools/pull_history_2023_2024_wnba_stats.R
suppressPackageStartupMessages({
  library(httr)
  library(jsonlite)
  library(readr)
  library(dplyr)
  library(stringr)
})

# ---------- config ----------
seasons <- c(2023, 2024)
out_dir <- "raw_data"
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)

# WNBA uses LeagueID=10 on stats.nba.com endpoints
BASE <- "https://stats.nba.com/stats"

common_headers <- add_headers(
  `User-Agent` = "Mozilla/5.0",
  `Accept` = "application/json, text/plain, */*",
  `Accept-Language` = "en-US,en;q=0.9",
  `Origin` = "https://www.nba.com",
  `Referer` = "https://www.nba.com/",
  `Connection` = "keep-alive"
)

get_json <- function(url, query) {
  resp <- GET(url, common_headers, query = query)
  if (status_code(resp) != 200) {
    stop(paste("HTTP", status_code(resp), "for", url, "\n", content(resp, "text", encoding = "UTF-8")))
  }
  txt <- content(resp, "text", encoding = "UTF-8")
  fromJSON(txt, simplifyDataFrame = TRUE)
}

resultset_to_df <- function(js, idx = 1) {
  rs <- js$resultSets[[idx]]
  headers <- rs$headers
  rows <- rs$rowSet
  df <- as.data.frame(rows, stringsAsFactors = FALSE)
  colnames(df) <- headers
  df
}

# ---------- pull shot location buckets (player-level) ----------
pull_shot_locations <- function(season) {
  url <- paste0(BASE, "/leaguedashplayershotlocations")

  # Season format on stats usually like "2023" for WNBA (works in practice for WNBA endpoints)
  # If you ever need "2023-24" style, we can adjust, but WNBA seasons are single-year.
  q <- list(
    LeagueID = "10",
    Season = as.character(season),
    SeasonType = "Regular Season",
    PerMode = "Totals",
    MeasureType = "Base",
    PaceAdjust = "N",
    PlusMinus = "N",
    Rank = "N",
    Outcome = "",
    Location = "",
    Month = "0",
    SeasonSegment = "",
    DateFrom = "",
    DateTo = "",
    OpponentTeamID = "0",
    VsConference = "",
    VsDivision = "",
    GameSegment = "",
    Period = "0",
    LastNGames = "0"
  )

  js <- get_json(url, q)
  df <- resultset_to_df(js, 1)

  # Keep only the columns we care about (buckets + ids)
  # These column names come standard from this endpoint:
  # PLAYER_ID, PLAYER_NAME, TEAM_ID,
  # Restricted Area / In The Paint (Non-RA) / Mid-Range / Corner 3 / Above the Break 3 etc.
  df <- df %>%
    transmute(
      season = season,
      player_id = as.character(PLAYER_ID),
      player_name = PLAYER_NAME,
      team_id = as.character(TEAM_ID),

      restricted_area_fgm = as.numeric(RESTRICTED_AREA_FGM),
      restricted_area_fga = as.numeric(RESTRICTED_AREA_FGA),

      paint_non_ra_fgm = as.numeric(IN_THE_PAINT_NON_RA_FGM),
      paint_non_ra_fga = as.numeric(IN_THE_PAINT_NON_RA_FGA),

      mid_range_fgm = as.numeric(MID_RANGE_FGM),
      mid_range_fga = as.numeric(MID_RANGE_FGA),

      corner_3_fgm = as.numeric(CORNER_3_FGM),
      corner_3_fga = as.numeric(CORNER_3_FGA),

      above_break_3_fgm = as.numeric(ABOVE_THE_BREAK_3_FGM),
      above_break_3_fga = as.numeric(ABOVE_THE_BREAK_3_FGA)
    )

  df
}

# ---------- pull player totals for minutes/games/usage proxies ----------
pull_player_totals <- function(season) {
  url <- paste0(BASE, "/leaguedashplayerstats")

  q <- list(
    LeagueID = "10",
    Season = as.character(season),
    SeasonType = "Regular Season",
    PerMode = "Totals",
    MeasureType = "Base",
    PaceAdjust = "N",
    PlusMinus = "N",
    Rank = "N",
    Outcome = "",
    Location = "",
    Month = "0",
    SeasonSegment = "",
    DateFrom = "",
    DateTo = "",
    OpponentTeamID = "0",
    VsConference = "",
    VsDivision = "",
    GameSegment = "",
    Period = "0",
    LastNGames = "0"
  )

  js <- get_json(url, q)
  df <- resultset_to_df(js, 1)

  # columns: PLAYER_ID, PLAYER_NAME, TEAM_ID, GP, MIN, FGA, FTA, TOV, AST, etc.
  df <- df %>%
    transmute(
      season = season,
      player_id = as.character(PLAYER_ID),
      player_name = PLAYER_NAME,
      team_id = as.character(TEAM_ID),

      games = as.numeric(GP),
      minutes = as.numeric(MIN),

      fga = as.numeric(FGA),
      fta = as.numeric(FTA),
      tov = as.numeric(TOV),
      ast = as.numeric(AST)
    ) %>%
    mutate(
      mpg = ifelse(games > 0, minutes / games, NA_real_),
      usage_proxy = ifelse(minutes > 0, (fga + 0.44 * fta + tov + ast) / minutes, NA_real_)
    )

  df
}

# ---------- run ----------
all_shots <- bind_rows(lapply(seasons, pull_shot_locations))
all_totals <- bind_rows(lapply(seasons, pull_player_totals))

write_csv(all_shots, file.path(out_dir, "history_shot_locations_2023_2024.csv"))
write_csv(all_totals, file.path(out_dir, "history_minutes_usage_2023_2024.csv"))

cat("✅ wrote:\n")
cat(" - raw_data/history_shot_locations_2023_2024.csv\n")
cat(" - raw_data/history_minutes_usage_2023_2024.csv\n")
