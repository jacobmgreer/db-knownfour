suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

dir.create("parquet", showWarnings = F)
dir.create("parquet/step1", showWarnings = F)
dir.create("parquet/step2", showWarnings = F)
dir.create("parquet/step3", showWarnings = F)

## Load wd-imdb

wd_imdb_people <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/people/wd_imdb_people.rds")) %>%
  group_by(nconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

write_parquet(wd_imdb_people, "wd_imdb_people.parquet")

wd_imdb_film <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/film/wd_imdb_films.rds")) %>%
  group_by(tconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

write_parquet(wd_imdb_film, "wd_imdb_film.parquet")

## Load Name Basics

imdb_names <-
  read_delim(
    "https://datasets.imdbws.com/name.basics.tsv.gz",
    delim = "\t", 
    escape_double = FALSE, 
    trim_ws = TRUE) %>%
  reframe(nconst, primaryName, primaryProfession, knownForTitles)

write_parquet(imdb_names, "imdb_names.parquet")

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

write_parquet(imdb_basics, "imdb_basics.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Loaded IMDb Content")

### Batch Out Unknown Profession

unknown_profession <-
  imdb_names %>%
  filter(primaryProfession == "\\N") %>%
  reframe(nconst)

write_parquet(unknown_profession, "parquet/step1/unknown_profession.parquet")

imdb_names <-
  imdb_names %>%
  filter(!nconst %in% unknown_profession$nconst)

## Create KnownFor3 Profession Summary

KF3_professions <-
  imdb_names %>%
  reframe(nconst, knownForProfession = primaryProfession) %>%
  separate_longer_delim(knownForProfession, delim = ",")

## KF3 Profession Summary

profession_count <-
  KF3_professions %>%
  count(knownForProfession)
write_csv(profession_count, "parquet/step1/profession_count.csv")

## Batch K3 Professions

write_parquet(KF3_professions, "parquet/step1/KF3.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created Unknown Professions")

## Create KF4 Data Frame

KF4_content <-
  imdb_names %>%
  reframe(nconst, knownForTitles) %>%
  separate_longer_delim(knownForTitles, delim = ",") %>%
  left_join(wd_imdb_film, by = c("knownForTitles" = "tconst"))

write_parquet(KF4_content, "K4_content.parquet")

## Batch Out Non-KF4 Content

unknown_for <-
  imdb_basics %>%
  filter(!tconst %in% KF4_content$knownForTitles) %>%
  left_join(wd_imdb_film, by = "tconst")

write_parquet(
  unknown_for %>% reframe(tconst, iType, iYear, isAdult, QID), 
  "parquet/step1/unknown_for.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created Unknown Content")