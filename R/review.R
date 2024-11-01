suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

wd_imdb_people <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/people/wd_imdb_people.rds")) %>%
  group_by(nconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

wd_imdb_film <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/film/wd_imdb_films.rds")) %>%
  group_by(tconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

review_KF4 <-
  list.files("parquet/KF4", recursive = TRUE, full.names = TRUE) %>%
  map_dfr(., read_parquet) %>%
  left_join(
    wd_imdb_people,
    by = "nconst"
  )

review_KF3 <-
  list.files("parquet/KF3", recursive = TRUE, full.names = TRUE) %>%
  map_dfr(., read_parquet) %>%
  reframe(nconst, knownForProfession) %>%
  left_join(review_KF4, by = "nconst")

knownFour_summary <-
  review_KF3 %>%
  filter(!is.na(content)) %>%
  group_by(knownForProfession) %>%
  summarize(
    people = n_distinct(nconst),
    credits = sum(content, na.rm = T),
    in_wikidata = n_distinct(QID),
    minReleaseYear = min(minReleaseYear, na.rm = T),
    maxReleaseYear = max(maxReleaseYear, na.rm = T),
    anyAdult = sum(anyAdult, na.rm = T),
    anyTV = sum(anyTV, na.rm = T),
    anyGame = sum(anyGame, na.rm = T),
    anyShort = sum(anyShort, na.rm = T),
    anyFilm = sum(anyFilm, na.rm = T),
    anyVideo = sum(anyVideo, na.rm = T)
  )

knownFour_k4s <-
  review_KF3 %>%
  filter(!is.na(content)) %>%
  group_by(knownForProfession, content) %>%
  summarize(
    people = n_distinct(nconst),
    credits = sum(content, na.rm = T),
    in_wikidata = n_distinct(QID),
    minReleaseYear = min(minReleaseYear, na.rm = T),
    maxReleaseYear = max(maxReleaseYear, na.rm = T),
    anyAdult = sum(anyAdult, na.rm = T),
    anyTV = sum(anyTV, na.rm = T),
    anyGame = sum(anyGame, na.rm = T),
    anyShort = sum(anyShort, na.rm = T),
    anyFilm = sum(anyFilm, na.rm = T),
    anyVideo = sum(anyVideo, na.rm = T)
  )

knownFours <-
  bind_rows(
    knownFour_k4s %>%
      mutate(content = as.character(content)),
    knownFour_summary %>%
      mutate(content = "Summary")
  ) %>%
  arrange(knownForProfession, content)

write_csv(knownFours, "knownfours.csv")

review_unknown_for <-
  list.files("parquet/unknown_for", recursive = TRUE, full.names = TRUE) %>%
  map_dfr(., read_parquet) %>%
  reframe(tconst, iType, iYear, isAdult)

review_unknown_profession <-
  list.files("parquet/unknown_profession", recursive = TRUE, full.names = TRUE) %>%
  map_dfr(., read_parquet) %>%
  reframe(nconst)