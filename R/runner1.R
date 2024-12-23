suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

dir.create("parquet", showWarnings = F)
dir.create("parquet/step1", showWarnings = F)
dir.create("parquet/step2", showWarnings = F)
dir.create("parquet/step3", showWarnings = F)

## Load wd-imdb

wd_imdb_film <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/film/wd_imdb_films.rds")) %>%
  group_by(tconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

## Load Name Basics

imdb_names <-
  read_delim(
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    delim = "\t", 
    escape_double = FALSE, 
    trim_ws = TRUE) %>%
  reframe(nconst, primaryName, primaryProfession, knownForTitles)

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
    isAdult)

message(format(Sys.time(), '%H:%M:%S'), " - ", "Loaded IMDb Content")

### Batch Out Unknown Profession

unknown_profession <-
  imdb_names %>%
  filter(primaryProfession == "\\N") %>%
  reframe(nconst)

write_parquet(unknown_profession, "parquet/step1/unknown_profession.parquet")

## Create KnownFor3 Profession Summary

KF3_professions <-
  imdb_names %>%
  reframe(nconst, knownForProfession = primaryProfession) %>%
  separate_longer_delim(knownForProfession, delim = ",")

write_parquet(KF3_professions, "parquet/step1/KF3.parquet")

## KF3 Profession Summary

profession_count <-
  KF3_professions %>%
  count(knownForProfession)

write_csv(profession_count, "parquet/step1/profession_count.csv")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF3 Professions")

## Create KF4 Data Frame

KF4_content <-
  imdb_names %>%
  filter(knownForTitles != "\\N") %>%
  reframe(nconst, tconst = knownForTitles) %>%
  separate_longer_delim(tconst, delim = ",") %>%
  left_join(wd_imdb_film, by = "tconst") %>%
  left_join(imdb_basics, by = "tconst")

KF4_TV <-
  KF4_content %>% filter(iType %in% c("tvEpisode", "tvSeries", "tvShort", "tvMiniSeries", "tvSpecial", "tvMovie", "tvPilot"))
write_parquet(KF4_TV, "parquet/step1/KF4_TV.parquet")

KF4_VideoGames <-
  KF4_content %>% filter(iType == "videoGame")
write_parquet(KF4_TV, "parquet/step1/KF4_VideoGames.parquet")

KF4_Shorts <-
  KF4_content %>% filter(iType == "short")
write_parquet(KF4_TV, "parquet/step1/KF4_Shorts.parquet")

KF4_Videos <-
  KF4_content %>% filter(iType == "video")
write_parquet(KF4_TV, "parquet/step1/KF4_Videos.parquet")

KF4_Movies <-
  KF4_content %>% filter(iType == "movie")
write_parquet(KF4_TV, "parquet/step1/KF4_Movies.parquet")

KF4_isAdult <-
  KF4_content %>% filter(isAdult == 1)
write_parquet(KF4_TV, "parquet/step1/KF4_isAdult.parquet")


## Batch Out Non-KF4 Content

unknown_for <-
  imdb_basics %>%
  filter(!tconst %in% KF4_content$knownForTitles) %>%
  left_join(wd_imdb_film, by = "tconst")

write_parquet(
  unknown_for %>% reframe(tconst, iType, iYear, isAdult, QID), 
  "parquet/step1/unknown_for.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created Unknown Content")