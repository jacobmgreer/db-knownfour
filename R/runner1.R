suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

dir.create("KF4", showWarnings = F)
dir.create("KF4/basics", showWarnings = F)
dir.create("KF4/titles", showWarnings = F)
dir.create("KF4/people", showWarnings = F)

## Load wd-imdb

wd_imdb_film <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/film/wd_imdb_films.rds")) %>%
  group_by(tconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

wd_imdb_people <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/people/wd_imdb_people.rds")) %>%
  group_by(nconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

message(format(Sys.time(), '%H:%M:%S'), " - ", "Loaded Wikidata Queries")

## Load Name Basics

imdb_names <-
  read_delim(
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    delim = "\t", 
    escape_double = FALSE, 
    trim_ws = TRUE) %>%
  reframe(nconst, primaryName, primaryProfession, knownForTitles) %>%
  left_join(wd_imdb_people, by = "nconst")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Loaded IMDb Names")

## Load Title Basics

imdb_basics <-
  read_delim(
    "https://datasets.imdbws.com/title.basics.tsv.gz",
    delim = "\t", 
    escape_double = FALSE, 
    trim_ws = TRUE) %>%
  reframe(
    tconst, 
    iType = titleType, 
    iYear = as.numeric(ifelse(startYear == "\\N", NA, startYear)), 
    isAdult) %>%
  left_join(wd_imdb_film, by = "tconst")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Loaded IMDb Content")

rm(wd_imdb_film, wd_imdb_people)

### Batch Out Unknown Profession

write_parquet(
  imdb_names %>%
    filter(primaryProfession == "\\N") %>%
    reframe(nconst, QID), 
  "KF4/basics/unknown_profession.parquet")

## Create KnownFor3 Profession Summary

write_parquet(
  imdb_names %>%
    reframe(nconst, QID, knownForProfession = primaryProfession) %>%
    separate_longer_delim(knownForProfession, delim = ","), 
  "KF4/basics/KF3.parquet")

## KF3 Profession Summary

write_parquet(
  read_parquet("KF4/KF3.parquet") %>%
    group_by(knownForProfession) %>%
    summarize(
      n = n(),
      nQID = n_distinct(nconst[!is.na(QID)]),
      per_nQID = round(nQID / n * 100, digits = 2)
    ), 
  "KF4/basics/profession_count.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF3 Professions")

## Create KF4 Data Frame

KF4_content <-
  imdb_names %>%
  filter(knownForTitles != "\\N") %>%
  reframe(nconst, tconst = knownForTitles) %>%
  separate_longer_delim(tconst, delim = ",") %>%
  left_join(imdb_basics, by = "tconst")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF4 Content")

## Batch Out Non-KF4 Content

write_parquet(
  imdb_basics %>%
    filter(!tconst %in% KF4_content$knownForTitles) %>% 
    reframe(tconst, iType, iYear, isAdult, QID), 
  "KF4/basics/unknown_for.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created Unknown Content")

rm(imdb_basics, imdb_names)

KF4_TV <-
  KF4_content %>% filter(iType %in% c("tvEpisode", "tvSeries", "tvShort", "tvMiniSeries", "tvSpecial", "tvMovie", "tvPilot"))
write_parquet(
  KF4_content %>% 
    filter(iType %in% 
      c(
        "tvEpisode", 
        "tvSeries", 
        "tvShort", 
        "tvMiniSeries", 
        "tvSpecial", 
        "tvMovie", 
        "tvPilot"
      )
    ), 
  "KF4/titles/KF4_TV.parquet")

write_parquet(
  KF4_content %>% 
    filter(iType == "videoGame"), 
  "KF4/titles/KF4_VideoGames.parquet")

write_parquet(
  KF4_content %>% 
    filter(iType == "short"), 
  "KF4/titles/KF4_Shorts.parquet")

write_parquet(
  KF4_content %>% 
    filter(iType == "video"), 
  "KF4/titles/KF4_Videos.parquet")

write_parquet(
  KF4_content %>% 
    filter(iType == "movie"), 
  "KF4/titles/KF4_Movies.parquet")

write_parquet(
  KF4_content %>% 
    filter(isAdult == 1), 
  "KF4/titles/KF4_isAdult.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Batched KF4 Title Sets")

rm(list=ls())

for (i in list.files("basics/KF4", pattern = "*.parquet", full.names = T)) {
  write_parquet(
    read_parquet(i) %>%
      arrange(iYear, nconst) %>%
      group_by(nconst) %>%
      summarize(
        content = n_distinct(tconst),
        contentQID = n_distinct(QID, na.rm = T),
        minReleaseYear = min(iYear, na.rm = T),
        maxReleaseYear = max(iYear, na.rm = T)
      ), 
    paste0("KF4/people/", basename(i)))
}

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF4 People Sets")