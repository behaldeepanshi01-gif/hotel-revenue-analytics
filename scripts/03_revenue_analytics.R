# =============================================================================
# 03_revenue_analytics.R
# Hotel Revenue Analytics Pipeline - KPI Calculations & Pivoting
#
# Calculates hotel industry KPIs (ADR, RevPAR, Occupancy, ALOS) and
# demonstrates pivot_wider / pivot_longer for analysis and visualization prep.
# =============================================================================

library(tidyverse)
library(lubridate)

cat("=" |> strrep(60), "\n")
cat("HOTEL REVENUE ANALYTICS - KPI CALCULATIONS\n")
cat("=" |> strrep(60), "\n\n")

# --- Load Star Schema --------------------------------------------------------

fact_bookings <- read_csv("data/processed/fact_bookings.csv",
                           show_col_types = FALSE)
dim_guest     <- read_csv("data/processed/dim_guest.csv",
                           show_col_types = FALSE)
dim_room      <- read_csv("data/processed/dim_room.csv",
                           show_col_types = FALSE)
dim_date      <- read_csv("data/processed/dim_date.csv",
                           show_col_types = FALSE)
dim_channel   <- read_csv("data/processed/dim_channel.csv",
                           show_col_types = FALSE)
dim_rate_code <- read_csv("data/processed/dim_rate_code.csv",
                           show_col_types = FALSE)

cat("Star schema loaded.\n")
cat("Fact table rows:", nrow(fact_bookings), "\n\n")

# Hotel configuration
hotel_capacity <- 250

# --- Build Denormalized View for Analysis ------------------------------------

bookings_full <- fact_bookings %>%
  left_join(dim_guest, by = "guest_key") %>%
  left_join(dim_room, by = "room_key") %>%
  left_join(dim_date, by = "date_key") %>%
  left_join(dim_channel, by = "channel_key") %>%
  left_join(dim_rate_code, by = "rate_key") %>%
  mutate(
    check_in_date = as.Date(check_in_date),
    check_out_date = as.Date(check_out_date)
  )

cat("Denormalized view built:", nrow(bookings_full), "rows\n\n")

# =============================================================================
# KPI 1: Monthly Performance Metrics
# =============================================================================

cat("--- MONTHLY KPIs ---\n")

# Only non-cancelled bookings for revenue metrics
active_bookings <- bookings_full %>% filter(!is_cancelled)

monthly_kpis <- active_bookings %>%
  group_by(month, month_name) %>%
  summarise(
    total_bookings = n(),
    total_room_nights = sum(nights),
    total_revenue = sum(total_revenue),
    # ADR = Average Daily Rate (total room revenue / rooms sold)
    ADR = round(sum(total_revenue) / sum(nights), 2),
    # Available room nights for the month
    .groups = "drop"
  ) %>%
  mutate(
    # Days in each month of 2025
    days_in_month = days_in_month(ymd(paste0("2025-", month, "-01"))),
    available_rooms = hotel_capacity * days_in_month,
    # Occupancy Rate = rooms sold / rooms available
    occupancy_rate = round(total_room_nights / available_rooms * 100, 1),
    # RevPAR = Revenue Per Available Room (ADR * Occupancy)
    RevPAR = round(ADR * (occupancy_rate / 100), 2),
    # ALOS = Average Length of Stay
    ALOS = round(total_room_nights / total_bookings, 2)
  )

cat("\nMonthly Performance:\n")
monthly_kpis %>%
  select(month_name, total_bookings, ADR, occupancy_rate, RevPAR, ALOS) %>%
  print(n = 12)

# =============================================================================
# KPI 2: Channel Mix Analysis
# =============================================================================

cat("\n--- CHANNEL MIX ANALYSIS ---\n")

channel_performance <- active_bookings %>%
  group_by(channel_name, channel_category) %>%
  summarise(
    bookings = n(),
    revenue = sum(total_revenue),
    avg_daily_rate = round(mean(daily_rate), 2),
    avg_lead_days = round(mean(booking_lead_days), 1),
    avg_los = round(mean(nights), 2),
    .groups = "drop"
  ) %>%
  mutate(
    revenue_share = round(revenue / sum(revenue) * 100, 1),
    booking_share = round(bookings / sum(bookings) * 100, 1)
  ) %>%
  arrange(desc(revenue))

cat("\nChannel Performance:\n")
print(channel_performance)

# Direct vs OTA vs Other
channel_category_summary <- channel_performance %>%
  group_by(channel_category) %>%
  summarise(
    total_revenue = sum(revenue),
    total_bookings = sum(bookings),
    .groups = "drop"
  ) %>%
  mutate(
    revenue_pct = round(total_revenue / sum(total_revenue) * 100, 1)
  )

cat("\nDirect vs OTA vs Indirect/Group:\n")
print(channel_category_summary)

# =============================================================================
# KPI 3: Cancellation Rate by Channel
# =============================================================================

cat("\n--- CANCELLATION ANALYSIS ---\n")

cancellation_by_channel <- bookings_full %>%
  group_by(channel_name) %>%
  summarise(
    total_bookings = n(),
    cancelled = sum(is_cancelled),
    cancellation_rate = round(cancelled / total_bookings * 100, 1),
    lost_revenue = sum(total_revenue[is_cancelled]),
    .groups = "drop"
  ) %>%
  arrange(desc(cancellation_rate))

cat("\nCancellation Rates by Channel:\n")
print(cancellation_by_channel)

# =============================================================================
# KPI 4: Loyalty Tier Performance
# =============================================================================

cat("\n--- LOYALTY TIER PERFORMANCE ---\n")

loyalty_performance <- active_bookings %>%
  group_by(loyalty_tier) %>%
  summarise(
    guests = n_distinct(guest_name),
    bookings = n(),
    total_revenue = sum(total_revenue),
    avg_daily_rate = round(mean(daily_rate), 2),
    avg_los = round(mean(nights), 2),
    avg_spend = round(sum(total_revenue) / n(), 2),
    .groups = "drop"
  ) %>%
  mutate(
    revenue_share = round(total_revenue / sum(total_revenue) * 100, 1)
  ) %>%
  arrange(desc(avg_spend))

cat("\nLoyalty Tier Performance:\n")
print(loyalty_performance)

# =============================================================================
# PIVOT_WIDER: Revenue by Room Type per Quarter
# =============================================================================

cat("\n--- PIVOT_WIDER: Revenue by Room Type x Quarter ---\n")

revenue_by_room_quarter <- active_bookings %>%
  group_by(room_type_code, quarter_label) %>%
  summarise(
    revenue = round(sum(total_revenue), 0),
    .groups = "drop"
  ) %>%
  # pivot_wider: each quarter becomes its own column
  pivot_wider(
    names_from = quarter_label,
    values_from = revenue,
    values_fill = 0
  ) %>%
  # Add row total
  mutate(Total = Q1 + Q2 + Q3 + Q4) %>%
  arrange(desc(Total))

cat("\nRevenue by Room Type per Quarter (pivot_wider):\n")
print(revenue_by_room_quarter)

# =============================================================================
# PIVOT_LONGER: Monthly KPIs for Faceted Plotting
# =============================================================================

cat("\n--- PIVOT_LONGER: KPIs for Faceted Plotting ---\n")

kpi_long <- monthly_kpis %>%
  select(month, month_name, ADR, RevPAR, occupancy_rate, ALOS) %>%
  # pivot_longer: reshape KPIs from wide to long for faceting
  pivot_longer(
    cols = c(ADR, RevPAR, occupancy_rate, ALOS),
    names_to = "kpi_name",
    values_to = "kpi_value"
  ) %>%
  mutate(
    kpi_label = case_when(
      kpi_name == "ADR"            ~ "ADR ($)",
      kpi_name == "RevPAR"         ~ "RevPAR ($)",
      kpi_name == "occupancy_rate" ~ "Occupancy Rate (%)",
      kpi_name == "ALOS"           ~ "Avg Length of Stay (nights)"
    )
  )

cat("\nKPI Long Format (first 12 rows):\n")
print(head(kpi_long, 12))

# =============================================================================
# Additional: Seasonal Analysis
# =============================================================================

cat("\n--- SEASONAL ANALYSIS ---\n")

seasonal_revenue <- active_bookings %>%
  group_by(season) %>%
  summarise(
    bookings = n(),
    revenue = sum(total_revenue),
    avg_rate = round(mean(daily_rate), 2),
    .groups = "drop"
  ) %>%
  mutate(
    season = factor(season, levels = c("Spring", "Summer", "Fall", "Winter")),
    revenue_share = round(revenue / sum(revenue) * 100, 1)
  ) %>%
  arrange(season)

cat("\nSeasonal Revenue:\n")
print(seasonal_revenue)

# =============================================================================
# Save analytics results for visualization script
# =============================================================================

cat("\n\nSaving analytics results...\n")

write_csv(monthly_kpis, "data/processed/analytics_monthly_kpis.csv")
write_csv(channel_performance, "data/processed/analytics_channel_performance.csv")
write_csv(cancellation_by_channel, "data/processed/analytics_cancellation.csv")
write_csv(loyalty_performance, "data/processed/analytics_loyalty.csv")
write_csv(revenue_by_room_quarter, "data/processed/analytics_room_quarter.csv")
write_csv(kpi_long, "data/processed/analytics_kpi_long.csv")
write_csv(seasonal_revenue, "data/processed/analytics_seasonal.csv")

# Save the full denormalized view for visualization use
write_csv(bookings_full, "data/processed/bookings_denormalized.csv")

cat("All analytics tables saved to data/processed/\n")
cat("\n")
cat("=" |> strrep(60), "\n")
cat("REVENUE ANALYTICS COMPLETE\n")
cat("=" |> strrep(60), "\n")
