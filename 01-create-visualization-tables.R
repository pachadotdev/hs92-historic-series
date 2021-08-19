source("99-pkgs-funs-dirs.R")

d_hs <- open_dataset("hs92-historic",
  partitioning = c("aggregate_level", "trade_flow", "year", "reporter_iso"))

# YRPC ------------------------------------------------------------------

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    # exp ----

    d_exp <- d_hs %>%
      filter(
        aggregate_level == "aggregate_level=4",
        trade_flow == "trade_flow=export",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        aggregate_level = remove_hive(aggregate_level),
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_reexp <- d_hs %>%
      filter(
        aggregate_level == "aggregate_level=4",
        trade_flow == "trade_flow=re-export",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_exp_corrected <- d_exp %>%
      left_join(d_reexp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      mutate_if(is.numeric, na_to_0) %>%
      rowwise() %>%
      mutate(
        trade_value_usd = max(trade_value_usd.x - trade_value_usd.y, 0)
      ) %>%
      ungroup() %>%
      select(-c(trade_value_usd.x, trade_value_usd.y))

    # imp ----

    d_imp <- d_hs %>%
      filter(
        aggregate_level == "aggregate_level=4",
        trade_flow == "trade_flow=import",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        aggregate_level = remove_hive(aggregate_level),
        trade_flow = remove_hive(trade_flow),
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(aggregate_level, trade_flow, year, reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_reimp <- d_hs %>%
      filter(
        aggregate_level == "aggregate_level=4",
        trade_flow == "trade_flow=re-import",
        year == y2
      ) %>%
      collect() %>%
      mutate(
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)

    d_imp_corrected <- d_imp %>%
      left_join(d_reimp, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      mutate_if(is.numeric, na_to_0) %>%
      rowwise() %>%
      mutate(
        trade_value_usd = max(trade_value_usd.x - trade_value_usd.y, 0)
      ) %>%
      ungroup() %>%
      select(reporter_iso, partner_iso, commodity_code, trade_value_usd)

    rm(d_exp, d_reexp, d_imp, d_reimp); gc()

    # full ----

    d_exp_corrected %>%
      select(-c(aggregate_level, trade_flow)) %>%
      full_join(d_imp_corrected, by = c("reporter_iso", "partner_iso", "commodity_code")) %>%
      rename(
        trade_value_usd_exp = trade_value_usd.x,
        trade_value_usd_imp = trade_value_usd.y
      ) %>%
      mutate(year = y) %>%
      select(year, everything()) %>%
      mutate(
        trade_value_usd_exp = na_to_0(trade_value_usd_exp),
        trade_value_usd_imp = na_to_0(trade_value_usd_imp),
        trade_value_usd_exc = trade_value_usd_exp + trade_value_usd_imp
      ) %>%
      filter(trade_value_usd_exc > 0) %>%
      select(-trade_value_usd_exc) %>%
      group_by(year, reporter_iso) %>%
      write_dataset("hs92-visualization/yrpc", hive_style = T)

    rm(d_exp_corrected, d_imp_corrected); gc()
  }
)

# YRP ------------------------------------------------------------------

d_yrpc <- open_dataset("hs92-visualization/yrpc",
                       partitioning = c("year","reporter_iso"))

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      group_by(year, reporter_iso, partner_iso) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      write_dataset("hs92-visualization/yrp", hive_style = T)
  }
)

# YRC ------------------------------------------------------------------

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      group_by(year, reporter_iso, commodity_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year, reporter_iso) %>%
      write_dataset("hs92-visualization/yrc", hive_style = T)
  }
)

# YR -------------------------------------------------------------------

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d_yr <- d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      group_by(year, reporter_iso) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      )

    d_yr %>%
      group_by(year) %>%
      write_dataset("hs92-visualization/yr", hive_style = T)
  }
)

# YC -------------------------------------------------------------------

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d_yrpc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
      ) %>%
      group_by(year, commodity_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year) %>%
      write_dataset("hs92-visualization/yc", hive_style = T)
  }
)

# Attributes -------------------------------------------------------------------

load("../observatory-codes/02-2-product-data-tidy/hs92-product-names.RData")

hs_product_names_92 <- hs_product_names %>%
  select(commodity_code = hs, commodity_shortname_english = product_name) %>%
  filter(str_length(commodity_code) == 4)

load("../comtrade-codes/01-2-tidy-country-data/country-codes.RData")

attributes_countries <- country_codes %>%
  select(
    iso3_digit_alpha, contains("name"), country_abbrevation,
    contains("continent"), eu28_member
  ) %>%
  rename(
    country_iso = iso3_digit_alpha,
    country_abbreviation = country_abbrevation
  ) %>%
  mutate(country_iso = str_to_lower(country_iso)) %>%
  filter(country_iso != "null") %>%
  distinct(country_iso, .keep_all = T) %>%
  select(-country_abbreviation)

load("../comtrade-codes/02-2-tidy-product-data/product-codes.RData")

product_names <- product_codes %>%
  filter(
    classification == "H0",
    str_length(code) == 4
  ) %>%
  select(commodity_code = code, commodity_fullname_english = description) %>%
  mutate(group_code = str_sub(commodity_code, 1, 2))

product_names_2 <- product_codes %>%
  filter(
    classification == "H0",
    str_length(code) == 2
  ) %>%
  select(group_code = code, group_name = description)

product_names_3 <- product_names %>%
  left_join(hs_product_names, by = c("commodity_code" = "hs")) %>%
  rename(
    community_code = group_id,
    community_name = group_name
  ) %>%
  select(commodity_code, community_code, community_name)

colors <- product_names_3 %>%
  select(community_code) %>%
  distinct() %>%
  mutate(community_color = c(
    "#74c0e2", "#406662", "#549e95", "#8abdb6", "#bcd8af",
    "#a8c380", "#ede788", "#d6c650", "#dc8e7a", "#d05555",
    "#bf3251", "#872a41", "#993f7b", "#7454a6", "#a17cb0",
    "#d1a1bc", "#a1aafb", "#5c57d9", "#1c26b3", "#4d6fd0",
    "#7485aa", "#635b56"
  ))

attributes_commodities <- product_names %>%
  left_join(product_names_2) %>%
  select(commodity_code, commodity_fullname_english,
         group_code, group_fullname_english = group_name)

attributes_communities <- product_names_3 %>%
  left_join(colors)

attributes_commodities_shortnames <- attributes_commodities %>%
  select(commodity_code, commodity_fullname_english) %>%
  left_join(hs_product_names_92)

try(dir.create("hs92-visualization/attributes"))
write_parquet(attributes_countries, "hs92-visualization/attributes/countries.parquet")
write_parquet(attributes_products, "hs92-visualization/attributes/commodities.parquet")
write_parquet(attributes_communities, "hs92-visualization/attributes/communities.parquet")
write_parquet(attributes_products_shortnames, "hs92-visualization/attributes/commodities_shortnames.parquet")

# YR-Communities ---------------------------------------------------------------

d_yrc <- open_dataset("hs92-visualization/yrc",
                      partitioning = c("year", "reporter_iso"))

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d_yrc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      left_join(attributes_communities) %>%
      group_by(year, reporter_iso, community_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year) %>%
      write_dataset("hs92-visualization/yr-communities", hive_style = T)
  }
)

# YR-Groups ------------------------------------------------------------------

map(
  1962:2020,
  function(y) {
    message(y)

    y2 <- paste0("year=", y)

    d_yrc %>%
      filter(
        year == y2
      ) %>%
      collect() %>%
      mutate(
        year = remove_hive(year),
        reporter_iso = remove_hive(reporter_iso)
      ) %>%
      left_join(attributes_commodities) %>%
      group_by(year, reporter_iso, group_code) %>%
      summarise(
        trade_value_usd_exp = sum(trade_value_usd_exp, na.rm = T),
        trade_value_usd_imp = sum(trade_value_usd_imp, na.rm = T)
      ) %>%
      group_by(year) %>%
      write_dataset("hs92-visualization/yr-groups", hive_style = T)
  }
)
