suppressMessages(library(tidyverse))
suppressMessages(library(arrow))
options(readr.show_col_types = FALSE)
options(warn=-1)

dir.create("KF4", showWarnings = F)

# Batch Out KF4 Content by Name Sets

KF4_types <- list.files("basics/KF4", pattern = "*.parquet", full.names = T)

for (i in KF4_types) {
  batch <-
    read_parquet(i) %>%
    arrange(iYear, nconst) %>%
      group_by(nconst) %>%
      summarize(
        content = n_distinct(tconst),
        contentQID = n_distinct(QID, na.rm = T),
        minReleaseYear = min(iYear, na.rm = T),
        maxReleaseYear = max(iYear, na.rm = T)
      )
  write_parquet(batch, paste0("KF4/", basename(i)))
}

write_parquet(KF4_batch, "parquet/step2/KF4.parquet")

message(format(Sys.time(), '%H:%M:%S'), " - ", "Created KF4 Name Sets")