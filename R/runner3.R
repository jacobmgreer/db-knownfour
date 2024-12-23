suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

## Generate knownFour dataset

wd_imdb_people <-
  readRDS(url("https://jacobmgreer.github.io/Nitrate-Datasets/datasets/people/wd_imdb_people.rds")) %>%
  group_by(nconst = imdb) %>%
  summarize(QID = paste(unique(QID), collapse = " || "))

review_KF4 <-
  read_parquet("parquet/step2/KF4.parquet") %>%
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
  read_parquet("parquet/step1/KF3.parquet") %>%
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

write_csv(knownFours, "parquet/step4/knownfours.csv")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Generate Known Four Dataset")