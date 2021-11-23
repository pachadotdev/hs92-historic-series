# hs92-historic-series

<!-- badges: start -->
<!-- badges: end -->

The goal of hs92-historic-series is to create curated international trade data since 1962. This uses the HS92 trade classification with 4 digits depth level.

Before 1988, this data uses SITC2 (>= 1976) and SITC1 (1962-1975) data. The source for this data is UN COMTRADE.

Instead of using CSV or RDS, the chosen format here is Apache Arrow, to force cross language and systems compatibility.

The data is obtained with the next procedure:

1. Take SITC 1, SITC 2 and  HS2 data for the period 1962-2020 (SITC 2 since 1976, HS 92 since 1988)
2. Convert each of the series to HS 92 by using the official conversion tables from UN COMTRADE
3. For each year, paste the three series, group by reporter, partner and commodity and take the maximum export/import value (i.e. if for 1990 a country hasn't implemented HS 92, then converted SITC 1/2 data will be used instead of showing no data for that country)
