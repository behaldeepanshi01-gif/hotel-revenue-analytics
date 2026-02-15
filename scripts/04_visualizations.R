# =============================================================================
# 04_visualizations.R
# Hotel Revenue Analytics Pipeline - Visualizations
#
# Creates 8 ggplot2 charts for the hotel revenue analytics dashboard.
# All plots are saved as PNG files to output/plots/.
# =============================================================================

library(tidyverse)
library(lubridate)
library(scales)

cat("=" |> strrep(60), "\n")
cat("HOTEL REVENUE ANALYTICS - VISUALIZATIONS\n")
cat("=" |> strrep(60), "\n\n")

# --- Load Analytics Data ------------------------------------------------------

monthly_kpis       <- read_csv("data/processed/analytics_monthly_kpis.csv",
                                show_col_types = FALSE)
channel_perf       <- read_csv("data/processed/analytics_channel_performance.csv",
                                show_col_types = FALSE)
cancellation_data  <- read_csv("data/processed/analytics_cancellation.csv",
                                show_col_types = FALSE)
loyalty_perf       <- read_csv("data/processed/analytics_loyalty.csv",
                                show_col_types = FALSE)
kpi_long           <- read_csv("data/processed/analytics_kpi_long.csv",
                                show_col_types = FALSE)
seasonal_data      <- read_csv("data/processed/analytics_seasonal.csv",
                                show_col_types = FALSE)
bookings_full      <- read_csv("data/processed/bookings_denormalized.csv",
                                show_col_types = FALSE)

# Filter to active (non-cancelled) bookings
active_bookings <- bookings_full %>% filter(!is_cancelled)

# Custom theme for all plots
theme_hotel <- theme_minimal(base_size = 13) +
  theme(
    plot.title = element_text(face = "bold", size = 15, hjust = 0),
    plot.subtitle = element_text(color = "gray40", size = 11),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

# Color palettes
hotel_colors <- c("#1B3A5C", "#2E86AB", "#A23B72", "#F18F01",
                  "#C73E1D", "#3B1F2B", "#44BBA4")
season_colors <- c("Spring" = "#7CB342", "Summer" = "#FDD835",
                    "Fall" = "#FF8F00", "Winter" = "#42A5F5")

cat("Data loaded. Generating 8 visualizations...\n\n")

# =============================================================================
# Plot 1: Monthly RevPAR Trend Line
# =============================================================================

cat("1/8 - Monthly RevPAR Trend...\n")

p1 <- ggplot(monthly_kpis, aes(x = month, y = RevPAR)) +
  geom_line(color = "#1B3A5C", linewidth = 1.2) +
  geom_point(color = "#1B3A5C", size = 3) +
  geom_text(aes(label = paste0("$", round(RevPAR))),
            vjust = -1.2, size = 3.5, color = "gray30") +
  scale_x_continuous(breaks = 1:12,
                     labels = month.abb) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Monthly RevPAR Trend - 2025",
    subtitle = "Revenue Per Available Room | 250-Room DC Hotel",
    x = NULL, y = "RevPAR ($)"
  ) +
  theme_hotel +
  coord_cartesian(ylim = c(0, max(monthly_kpis$RevPAR) * 1.15))

ggsave("output/plots/01_revpar_trend.png", p1,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Plot 2: ADR by Room Type (Bar Chart)
# =============================================================================

cat("2/8 - ADR by Room Type...\n")

adr_by_room <- active_bookings %>%
  group_by(room_type_code, room_type_name) %>%
  summarise(
    avg_daily_rate = round(mean(daily_rate), 2),
    bookings = n(),
    .groups = "drop"
  ) %>%
  mutate(
    room_label = paste0(room_type_code, "\n", room_type_name),
    room_type_code = factor(room_type_code,
                            levels = c("STD", "DBL", "KNG", "JRS", "STE"))
  ) %>%
  arrange(room_type_code)

p2 <- ggplot(adr_by_room, aes(x = room_type_code, y = avg_daily_rate,
                               fill = room_type_code)) +
  geom_col(width = 0.7, show.legend = FALSE) +
  geom_text(aes(label = dollar(avg_daily_rate)),
            vjust = -0.5, size = 4, fontface = "bold") +
  scale_fill_manual(values = hotel_colors[1:5]) +
  scale_y_continuous(labels = dollar_format(),
                     expand = expansion(mult = c(0, 0.15))) +
  labs(
    title = "Average Daily Rate by Room Type",
    subtitle = "Active bookings | Rack rates: STD $189, KNG $219, DBL $199, JRS $289, STE $399",
    x = "Room Type", y = "Average Daily Rate ($)"
  ) +
  theme_hotel

ggsave("output/plots/02_adr_by_room_type.png", p2,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Plot 3: Revenue by Channel (Stacked Quarterly)
# =============================================================================

cat("3/8 - Revenue by Channel (Quarterly)...\n")

channel_quarterly <- active_bookings %>%
  group_by(quarter_label, channel_name) %>%
  summarise(revenue = sum(total_revenue), .groups = "drop")

p3 <- ggplot(channel_quarterly, aes(x = quarter_label, y = revenue,
                                     fill = channel_name)) +
  geom_col(position = "stack", width = 0.7) +
  scale_y_continuous(labels = dollar_format(scale = 0.001, suffix = "K")) +
  scale_fill_manual(values = hotel_colors) +
  labs(
    title = "Revenue by Booking Channel (Quarterly)",
    subtitle = "Stacked view showing channel contribution per quarter",
    x = "Quarter", y = "Total Revenue ($K)", fill = "Channel"
  ) +
  theme_hotel +
  theme(legend.position = "right")

ggsave("output/plots/03_revenue_by_channel.png", p3,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Plot 4: Occupancy Rate vs ADR Scatter
# =============================================================================

cat("4/8 - Occupancy vs ADR Scatter...\n")

p4 <- ggplot(monthly_kpis, aes(x = occupancy_rate, y = ADR)) +
  geom_point(aes(size = total_revenue), color = "#2E86AB", alpha = 0.8) +
  geom_text(aes(label = month.abb[month]),
            vjust = -1.2, size = 3.5, color = "gray30") +
  geom_smooth(method = "lm", se = TRUE, color = "#A23B72",
              linetype = "dashed", alpha = 0.2) +
  scale_x_continuous(labels = function(x) paste0(x, "%")) +
  scale_y_continuous(labels = dollar_format()) +
  scale_size_continuous(labels = dollar_format(scale = 0.001, suffix = "K"),
                        range = c(3, 10)) +
  labs(
    title = "Occupancy Rate vs Average Daily Rate",
    subtitle = "Monthly data points | Size = Total Revenue | Trendline shown",
    x = "Occupancy Rate (%)", y = "ADR ($)",
    size = "Revenue"
  ) +
  theme_hotel

ggsave("output/plots/04_occupancy_vs_adr.png", p4,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Plot 5: Cancellation Rate by Channel
# =============================================================================

cat("5/8 - Cancellation Rate by Channel...\n")

cancel_plot_data <- cancellation_data %>%
  mutate(channel_name = fct_reorder(channel_name, cancellation_rate))

p5 <- ggplot(cancel_plot_data, aes(x = channel_name, y = cancellation_rate,
                                    fill = cancellation_rate)) +
  geom_col(width = 0.7, show.legend = FALSE) +
  geom_text(aes(label = paste0(cancellation_rate, "%")),
            hjust = -0.2, size = 4, fontface = "bold") +
  scale_fill_gradient(low = "#44BBA4", high = "#C73E1D") +
  scale_y_continuous(expand = expansion(mult = c(0, 0.2))) +
  coord_flip() +
  labs(
    title = "Cancellation Rate by Booking Channel",
    subtitle = "Higher OTA cancellation rates impact net revenue",
    x = NULL, y = "Cancellation Rate (%)"
  ) +
  theme_hotel

ggsave("output/plots/05_cancellation_by_channel.png", p5,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Plot 6: Revenue by Season
# =============================================================================

cat("6/8 - Revenue by Season...\n")

seasonal_plot <- seasonal_data %>%
  mutate(season = factor(season, levels = c("Spring", "Summer", "Fall", "Winter")))

p6 <- ggplot(seasonal_plot, aes(x = season, y = revenue, fill = season)) +
  geom_col(width = 0.7, show.legend = FALSE) +
  geom_text(aes(label = paste0(dollar(revenue, scale = 0.001, suffix = "K"),
                                "\n(", revenue_share, "%)")),
            vjust = -0.3, size = 3.8, fontface = "bold") +
  scale_fill_manual(values = season_colors) +
  scale_y_continuous(labels = dollar_format(scale = 0.001, suffix = "K"),
                     expand = expansion(mult = c(0, 0.2))) +
  labs(
    title = "Total Revenue by Season",
    subtitle = "DC hotel demand peaks in Spring (cherry blossoms) and Fall (conventions)",
    x = NULL, y = "Total Revenue ($K)"
  ) +
  theme_hotel

ggsave("output/plots/06_revenue_by_season.png", p6,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Plot 7: KPI Dashboard (Faceted - uses pivot_longer output)
# =============================================================================

cat("7/8 - KPI Dashboard (Faceted)...\n")

kpi_plot_data <- kpi_long %>%
  mutate(
    kpi_label = factor(kpi_label,
                       levels = c("ADR ($)", "RevPAR ($)",
                                  "Occupancy Rate (%)",
                                  "Avg Length of Stay (nights)"))
  )

p7 <- ggplot(kpi_plot_data, aes(x = month, y = kpi_value)) +
  geom_line(color = "#1B3A5C", linewidth = 1) +
  geom_point(color = "#2E86AB", size = 2.5) +
  facet_wrap(~ kpi_label, scales = "free_y", ncol = 2) +
  scale_x_continuous(breaks = seq(2, 12, 2),
                     labels = month.abb[seq(2, 12, 2)]) +
  labs(
    title = "Monthly KPI Dashboard - 2025",
    subtitle = "Key performance indicators tracked across all 12 months (pivot_longer output)",
    x = NULL, y = NULL
  ) +
  theme_hotel +
  theme(
    strip.text = element_text(face = "bold", size = 11),
    strip.background = element_rect(fill = "gray95", color = NA)
  )

ggsave("output/plots/07_kpi_dashboard.png", p7,
       width = 12, height = 8, dpi = 300, bg = "white")

# =============================================================================
# Plot 8: Rate Distribution by Loyalty Tier (Boxplot)
# =============================================================================

cat("8/8 - Rate Distribution by Loyalty Tier...\n")

loyalty_order <- c("Diamond", "Gold", "Silver", "Blue", "None")
loyalty_colors <- c("Diamond" = "#1B3A5C", "Gold" = "#F18F01",
                     "Silver" = "#9E9E9E", "Blue" = "#2E86AB",
                     "None" = "#BDBDBD")

boxplot_data <- active_bookings %>%
  mutate(loyalty_tier = factor(loyalty_tier, levels = loyalty_order))

p8 <- ggplot(boxplot_data, aes(x = loyalty_tier, y = daily_rate,
                                fill = loyalty_tier)) +
  geom_boxplot(alpha = 0.8, outlier.alpha = 0.4, show.legend = FALSE) +
  stat_summary(fun = mean, geom = "point", shape = 18, size = 3,
               color = "red", show.legend = FALSE) +
  scale_fill_manual(values = loyalty_colors) +
  scale_y_continuous(labels = dollar_format()) +
  labs(
    title = "Daily Rate Distribution by Loyalty Tier",
    subtitle = "Boxplot with median (line) and mean (red diamond) | Higher tiers get premium rooms",
    x = "Loyalty Tier", y = "Daily Rate ($)"
  ) +
  theme_hotel

ggsave("output/plots/08_rate_by_loyalty.png", p8,
       width = 10, height = 6, dpi = 300, bg = "white")

# =============================================================================
# Summary
# =============================================================================

cat("\n")
cat("=" |> strrep(60), "\n")
cat("ALL 8 VISUALIZATIONS SAVED TO output/plots/\n")
cat("=" |> strrep(60), "\n")
cat("\nPlots generated:\n")
cat("  1. 01_revpar_trend.png          - Monthly RevPAR trend line\n")
cat("  2. 02_adr_by_room_type.png      - ADR by room type bar chart\n")
cat("  3. 03_revenue_by_channel.png    - Revenue by channel (stacked quarterly)\n")
cat("  4. 04_occupancy_vs_adr.png      - Occupancy vs ADR scatter\n")
cat("  5. 05_cancellation_by_channel.png - Cancellation rate by channel\n")
cat("  6. 06_revenue_by_season.png     - Revenue by season\n")
cat("  7. 07_kpi_dashboard.png         - KPI dashboard (faceted)\n")
cat("  8. 08_rate_by_loyalty.png       - Rate distribution by loyalty tier\n")
