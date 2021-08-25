source("99-pkgs-funs-dirs.R")

# conversion codes ----

load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")
load("../comtrade-codes/02-2-tidy-product-data/product-correlation.RData")

sitc1_to_sitc2 <- product_correlation %>%
  select(sitc1, sitc2) %>%
  arrange(sitc2) %>%
  distinct(sitc1, .keep_all = T)

sitc2_to_hs92 <- product_correlation %>%
  select(sitc2, hs92) %>%
  arrange(hs92) %>%
  distinct(sitc2, .keep_all = T)

# hs ----

d_hs <- open_dataset("../uncomtrade-datasets-arrow/hs-rev1992/parquet/",
                     partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

# chl2015 <- d_hs %>%
#   filter(
#     year == "year=2015",
#     reporter_iso == "reporter_iso=chl",
#     aggregate_level == "aggregate_level=6",
#     partner_iso != "wld"
#   ) %>%
#   select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd) %>%
#   collect() %>%
#   mutate(
#     aggregate_level = remove_hive(aggregate_level),
#     trade_flow = remove_hive(trade_flow),
#     year = remove_hive(year),
#     reporter_iso = remove_hive(reporter_iso)
#   )
#
# sort(unique(chl2015$reporter_iso))
# sort(unique(chl2015$partner_iso))

map(
  1988:2020,
  function(y) {
    message(y)

    y <- paste0("year=", y)

    d6 <- d_hs %>%
      filter(
        year == y,
        aggregate_level == "aggregate_level=6",
        partner_iso != "wld"
      ) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd) %>%
      collect() %>%
      mutate(
        aggregate_level = remove_hive(aggregate_level),
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      )

    d4 <- d6 %>%
      mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
      group_by(trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
      mutate(aggregate_level = 4) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d6 %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    d4 %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset("intermediate-data-hs92", hive_style = T)

    rm(d6, d4); gc()
  }
)

# sitc ----

d_sitc1 <- open_dataset("../uncomtrade-datasets-arrow/sitc-rev1/parquet/",
  partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

d_sitc2 <- open_dataset("../uncomtrade-datasets-arrow/sitc-rev2/parquet/",
  partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

sitc_to_hs92 <- function(y, c = "sitc1") {
  message(y)

  y2 <- paste0("year=", y)

  if (c == "sitc1") {
    d <- d_sitc1
  } else {
    d <- d_sitc2
  }

  d6 <- d %>%
    filter(
      year == y2,
      aggregate_level == "aggregate_level=5",
      partner_iso != "wld"
    ) %>%
    select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd) %>%
    collect() %>%
    mutate(
      aggregate_level = remove_hive(aggregate_level),
      trade_flow = remove_hive(trade_flow),
      year = remove_hive(year),
      reporter_iso = remove_hive(reporter_iso)
    )

  if (c == "sitc1") {
    d6 <- d6 %>%
      left_join(sitc1_to_sitc2, by = c("commodity_code" = "sitc1")) %>%
      left_join(sitc2_to_hs92, by = "sitc2")
  } else {
    d6 <- d6 %>%
      left_join(sitc2_to_hs92, by = c("commodity_code" = "sitc2"))
  }

  d6 <- d6 %>%
    select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code = hs92, trade_value_usd) %>%
    mutate(
      commodity_code = case_when(
        is.na(commodity_code) ~ "999999",
        TRUE ~ commodity_code
      )
    ) %>%
    group_by(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
    summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
    mutate(aggregate_level = 6) %>%
    select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

  missing_codes_count <- d6 %>%
    ungroup() %>%
    filter(is.na(commodity_code)) %>%
    count() %>%
    pull()

  stopifnot(missing_codes_count == 0)

  d4 <- d6 %>%
    mutate(commodity_code = str_sub(commodity_code, 1, 4)) %>%
    group_by(trade_flow, year, reporter_iso, partner_iso, commodity_code) %>%
    summarise(trade_value_usd = sum(trade_value_usd, na.rm = T)) %>%
    mutate(aggregate_level = 4) %>%
    select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

  d6 %>%
    group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
    write_dataset(out_dir, hive_style = T)

  out_d <- ifelse(c == "sitc1", "intermediate-data-sitc1", "intermediate-data-sitc2")

  d4 %>%
    group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
    write_dataset(out_d, hive_style = T)

  rm(d6, d4); gc()
}

map(
  1962:2020,
  sitc_to_hs92
)

map(
  1976:2020,
  sitc_to_hs92,
  c = "sitc2"
)

# consolidate ----

d_sitc1_2 <- open_dataset("intermediate-data-sitc1/",
                        partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

d_sitc2_2 <- open_dataset("intermediate-data-sitc2/",
                          partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

d_hs92_2 <- open_dataset("intermediate-data-hs92/",
                          partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

map(
  1962:2020,
  function(y) {
    message(y)

    d <- d_sitc1_2 %>%
      filter(year == paste0("year=", y)) %>%
      collect() %>%
      bind_rows(
        d_sitc2_2 %>%
          filter(year == paste0("year=", y)) %>%
          collect()
      ) %>%
      bind_rows(
        d_hs92_2 %>%
          filter(year == paste0("year=", y)) %>%
          collect()
      )

    d <- d %>%
      group_by(aggregate_level, trade_flow, reporter_iso, partner_iso, commodity_code) %>%
      summarise(trade_value_usd = max(trade_value_usd, na.rm = T))

    d <- d %>%
      ungroup() %>%
      mutate_if(is.character, function(x) gsub(".*=", "", x))

    d %>%
      mutate(year = y) %>%
      group_by(aggregate_level, trade_flow, year, reporter_iso) %>%
      write_dataset(out_dir, hive_style = T)

    rm(d); gc()
  }
)

