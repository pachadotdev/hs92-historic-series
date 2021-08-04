library(dplyr)
library(arrow)
library(purrr)
library(stringr)

out_dir <- "hs-rev1992-historic/parquet"
try(dir.create(out_dir, recursive = T))

remove_hive <- function(x) {
  gsub(".*=", "", x)
}

na_to_0 <- function(x) {
  ifelse(is.na(x), 0, x)
}

zero_to_na <- function(x) {
  ifelse(x == 0, NA, x)
}
