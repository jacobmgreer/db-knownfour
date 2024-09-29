suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

unlink("parquet", force = T, recursive = T)
dir.create("parquet", showWarnings = F)
dir.create("parquet/KF3", showWarnings = F)
dir.create("parquet/KF4", showWarnings = F)
dir.create("parquet/unknown_for", showWarnings = F)
dir.create("parquet/unknown_profession", showWarnings = F)

## Load wd-imdb

wd_imdb_people <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/people/wd_imdb_people.rds")) %>%
  group_by(nconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

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
  reframe(nconst) %>%
  mutate(batch = ceiling(row_number()/500000))

for (j in unique(unknown_profession$batch)) {
  write_parquet(
    unknown_profession %>%
      filter(batch == j) %>%
      reframe(nconst), 
    paste0("parquet/unknown_profession/batch-", str_pad(j, 3, pad = "0"), ".parquet"))
}

imdb_names <-
  imdb_names %>%
  filter(!nconst %in% unknown_profession$nconst)

rm(j, unknown_profession)

## Create KnownFor3 Profession Summary

KF3_professions <-
  imdb_names %>%
  reframe(nconst, knownForProfession = primaryProfession) %>%
  separate_longer_delim(knownForProfession, delim = ",")

## KF3 Profession Summary

profession_count <-
  KF3_professions %>%
  count(knownForProfession)
write_csv(profession_count, "profession_count.csv")

## Batch K3 Professions

for (j in unique(KF3_professions$knownForProfession)) {
  batch <- 
    KF3_professions %>%
    filter(knownForProfession == j) %>%
    mutate(batch = ceiling(row_number()/500000))

  for (i in unique(batch$batch)) {
    write_parquet(
      batch %>%
        filter(batch == i) %>%
        reframe(nconst, knownForProfession), 
      paste0("parquet/KF3/", j, "-", str_pad(i, 3, pad = "0"), ".parquet"))
  }
}

rm(i, j, batch, KF3_professions, profession_count)

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created Unknown Professions")

## Create KF4 Data Frame

KF4_content <-
  imdb_names %>%
  reframe(nconst, knownForTitles) %>%
  separate_longer_delim(knownForTitles, delim = ",") %>%
  left_join(wd_imdb_film, by = c("knownForTitles" = "tconst"))

## Batch Out Non-KF4 Content

unknown_for <-
  imdb_basics %>%
  filter(!tconst %in% KF4_content$knownForTitles) %>%
  left_join(wd_imdb_film, by = "tconst") %>%
  mutate(batch = ceiling(row_number()/500000))

for (j in unique(unknown_for$batch)) {
  write_parquet(
    unknown_for %>%
      filter(batch == j) %>%
      reframe(tconst, iType, iYear, isAdult, QID), 
    paste0("parquet/unknown_for/batch-", str_pad(j, 3, pad = "0"), ".parquet"))
}

imdb_basics <-
  imdb_basics %>%
  filter(!tconst %in% unknown_for$tconst)

rm(j, unknown_for)

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created Unknown Content")

# Batch Out KF4 Content by Name Sets

imdb_names <-
  imdb_names %>%
  mutate(batch = ceiling(row_number()/300000))

for(j in unique(imdb_names$batch)) {
  batch <-
    imdb_names %>%
    filter(batch == j)

  KF4_batch <-
    KF4_content %>%
    filter(nconst %in% batch$nconst) %>%
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

  write_parquet(
    KF4_batch, 
    paste0("parquet/KF4/batch-", str_pad(j, 3, pad = "0"), ".parquet"))
  
  message(format(Sys.time(), '%H:%M:%S'), " - KF4 Name Set: ", j, "/", length(unique(imdb_names$batch)))
}
rm(j, batch, KF4_batch, KF4_content)

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created ALL KF4 Name Sets")

## Generate knownFour dataset

review_KF4 <-
  list.files("parquet/KF4", recursive = TRUE, full.names = TRUE) %>%
  map_dfr(., read_parquet) %>%
  left_join(wd_imdb_people, by = "nconst")

review_KF4_decades <-
  review_KF4 %>%
  separate_longer_delim(iYears, delim = ",") %>%
  reframe(
    nconst,
    iYears = 
      case_when(
        iYears == "NA" ~ NA,
        .default = floor(as.numeric(iYears)/10)*10)) %>%
  group_by(nconst, decade = iYears) %>%
  summarize(n = n()) %>%
  arrange(decade) %>%
  pivot_wider(names_from = decade, names_prefix = "decade_", values_from = n)

review_KF3 <-
  list.files("parquet/KF3", recursive = TRUE, full.names = TRUE) %>%
  map_dfr(., read_parquet) %>%
  left_join(review_KF4 %>% reframe(nconst, content), by = "nconst") %>%
  filter(!is.na(content))

review_KF3_decades_sum <-
  review_KF3 %>%
  left_join(review_KF4_decades, by = "nconst") %>%
  select(-nconst, content) %>%
  group_by(knownForProfession) %>% 
  summarise(across(everything(), ~ifelse(all(is.na(.)), NA, sum(., na.rm = T))))

review_KF3_decades_content <-
  review_KF3 %>%
  left_join(review_KF4_decades, by = "nconst") %>%
  select(-nconst) %>%
  group_by(knownForProfession, content) %>% 
  summarise(across(everything(), ~ifelse(all(is.na(.)), NA, sum(., na.rm = T))))

knownFour_summary <-
  review_KF3 %>%
  distinct(nconst, knownForProfession) %>%
  left_join(review_KF4, by = "nconst") %>%
  group_by(knownForProfession) %>%
  summarize(
    people = n_distinct(nconst),
    credits = sum(content, na.rm = T),
    in_wikidata = n_distinct(QID, na.rm = T),
    minKnownForYear = min(minReleaseYear, na.rm = T),
    maxKnownForYear = max(maxReleaseYear, na.rm = T),
    anyAdult = sum(anyAdult, na.rm = T),
    anyTV = sum(anyTV, na.rm = T),
    anyGame = sum(anyGame, na.rm = T),
    anyShort = sum(anyShort, na.rm = T),
    anyFilm = sum(anyFilm, na.rm = T),
    anyVideo = sum(anyVideo, na.rm = T)) %>%
  left_join(review_KF3_decades_sum, by = "knownForProfession") %>%
  mutate(content = "Summary")

knownFour_k4s <-
  review_KF3 %>%
  distinct(nconst, knownForProfession) %>%
  left_join(review_KF4, by = "nconst") %>%
  group_by(knownForProfession, content) %>%
  summarize(
    people = n_distinct(nconst),
    credits = sum(content, na.rm = T),
    in_wikidata = n_distinct(QID, na.rm = T),
    minKnownForYear = min(minReleaseYear, na.rm = T),
    maxKnownForYear = max(maxReleaseYear, na.rm = T),
    anyAdult = sum(anyAdult, na.rm = T),
    anyTV = sum(anyTV, na.rm = T),
    anyGame = sum(anyGame, na.rm = T),
    anyShort = sum(anyShort, na.rm = T),
    anyFilm = sum(anyFilm, na.rm = T),
    anyVideo = sum(anyVideo, na.rm = T)) %>%
  left_join(review_KF3_decades_content, by = c("knownForProfession", "content")) %>%
  mutate(content = as.character(content))

knownFours <-
  bind_rows(knownFour_k4s, knownFour_summary) %>%
  arrange(knownForProfession, content) %>%
  mutate(
    minKnownForYear = ifelse(is.infinite(minKnownForYear), NA, minKnownForYear),
    maxKnownForYear = ifelse(is.infinite(maxKnownForYear), NA, maxKnownForYear))

write_csv(knownFours, "knownfours.csv")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Generate Known Four Dataset")