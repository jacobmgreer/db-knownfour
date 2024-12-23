suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

unknown_for <- read_parquet("parquet/step1/unknown_for.parquet")

imdb_basics <-
  read_parquet("imdb_basics.parquet") %>%
  filter(!tconst %in% unknown_for$tconst)

# Batch Out KF4 Content by Name Sets

KF4_batch <-
  read_parquet("K4_content.parquet") %>%
  rename(tconst = knownForTitles) %>%
  left_join(imdb_basics, by = "tconst") %>%
  arrange(iYear, nconst) %>%
  group_by(nconst) %>%
  summarize(
    content = n_distinct(tconst),
    contentQID = n_distinct(QID, na.rm = T),
    iYears = paste(iYear, collapse = ","),
    tconsts = paste(tconst, collapse = ","),
    minReleaseYear = min(iYear, na.rm = T),
    maxReleaseYear = max(iYear, na.rm = T),
    anyAdult = n_distinct(tconst[isAdult == 1], na.rm = T),
    anyTV = n_distinct(tconst[iType %in% c("tvEpisode", "tvSeries", "tvShort", "tvMiniSeries", "tvSpecial", "tvMovie", "tvPilot")], na.rm = T),
    anyGame = n_distinct(tconst[iType == "videoGame"], na.rm = T),
    anyShort = n_distinct(tconst[iType %in% c("short")], na.rm = T),
    anyFilm = n_distinct(tconst[iType == "movie"], na.rm = T),
    anyVideo = n_distinct(tconst[iType == "video"], na.rm = T)
  )

write_parquet(KF4_batch, "parquet/step2/KF4.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF4 Name Sets")