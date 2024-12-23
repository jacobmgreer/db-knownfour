suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

# Batch Out KF4 Content by Name Sets

KF4_batch <-
  read_parquet("/Users/jakeob/Documents/GitHub/compile/db-knownfour/parquet/step1/KF4_isAdult.parquet") %>%
  arrange(iYear, nconst) %>%
  group_by(nconst) %>%
  summarize(
    content = n_distinct(tconst),
    contentQID = n_distinct(QID, na.rm = T),
    iYears = paste(iYear, collapse = ","),
    minReleaseYear = min(iYear, na.rm = T),
    maxReleaseYear = max(iYear, na.rm = T)
  )

write_parquet(KF4_batch, "parquet/step2/KF4.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF4 Name Sets")