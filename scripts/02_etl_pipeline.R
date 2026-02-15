# =============================================================================
# 02_etl_pipeline.R
# Hotel Revenue Analytics Pipeline - ETL (Extract, Transform, Load)
#
# Reads the messy PMS export, cleans and transforms data, then builds a
# star schema with 5 dimension tables and 1 fact table.
# =============================================================================

library(tidyverse)
library(lubridate)

cat("=" |> strrep(60), "\n")
cat("HOTEL REVENUE ANALYTICS - ETL PIPELINE\n")
cat("=" |> strrep(60), "\n\n")

# =============================================================================
# EXTRACT - Read raw data
# =============================================================================

cat("PHASE 1: EXTRACT\n")
cat("-" |> strrep(40), "\n")

raw_data <- read_csv("data/raw/hotel_bookings_raw.csv",
                      show_col_types = FALSE)

cat("Raw records loaded:", nrow(raw_data), "\n")
cat("Columns:", ncol(raw_data), "\n")
cat("Column names:", paste(names(raw_data), collapse = ", "), "\n\n")

# =============================================================================
# TRANSFORM - Clean and standardize
# =============================================================================

cat("PHASE 2: TRANSFORM\n")
cat("-" |> strrep(40), "\n")

# --- Step 1: Remove duplicates -----------------------------------------------
n_before <- nrow(raw_data)
clean_data <- raw_data %>% distinct()
n_after <- nrow(clean_data)
cat("Duplicates removed:", n_before - n_after, "\n")

# --- Step 2: Clean text fields -----------------------------------------------

clean_data <- clean_data %>%
  mutate(
    # Trim whitespace from all character columns
    across(where(is.character), str_trim),
    # Standardize guest names to Title Case
    guest_name = str_to_title(guest_name),
    # Standardize room type to UPPER
    room_type = str_to_upper(room_type),
    # Standardize rate code to UPPER (should already be, but ensure)
    rate_code = str_to_upper(rate_code)
  )

cat("Text fields cleaned (trimmed, standardized casing)\n")

# --- Step 3: Parse dates (mixed formats) -------------------------------------

# Handle mixed date formats: YYYY-MM-DD and MM/DD/YYYY
parse_mixed_date <- function(date_str) {
  # Try YYYY-MM-DD first, then MM/DD/YYYY
  parsed <- parse_date_time(date_str, orders = c("ymd", "mdy"), quiet = TRUE)
  as.Date(parsed)
}

clean_data <- clean_data %>%
  mutate(
    check_in_date = parse_mixed_date(check_in_date),
    check_out_date = parse_mixed_date(check_out_date)
  )

cat("Dates parsed (handled YYYY-MM-DD and MM/DD/YYYY formats)\n")

# --- Step 4: Standardize cancellation flag ------------------------------------

clean_data <- clean_data %>%
  mutate(
    is_cancelled = case_when(
      str_to_lower(cancellation_flag) %in% c("y", "yes", "1") ~ TRUE,
      str_to_lower(cancellation_flag) %in% c("n", "no", "0") ~ FALSE,
      TRUE ~ FALSE
    )
  ) %>%
  select(-cancellation_flag)

cat("Cancellation flag standardized to boolean\n")

# --- Step 5: Handle missing values --------------------------------------------

# Replace missing loyalty_tier with "None"
clean_data <- clean_data %>%
  mutate(
    loyalty_tier = replace_na(loyalty_tier, "None")
  )
cat("Missing loyalty_tier replaced with 'None':",
    sum(is.na(raw_data$loyalty_tier)), "values\n")

# Replace missing num_guests with median (2)
median_guests <- median(clean_data$num_guests, na.rm = TRUE)
clean_data <- clean_data %>%
  mutate(
    num_guests = replace_na(num_guests, median_guests)
  )
cat("Missing num_guests replaced with median:",
    sum(is.na(raw_data$num_guests)), "values\n")

# Recalculate nights from dates (more reliable than raw data)
clean_data <- clean_data %>%
  mutate(
    nights = as.integer(check_out_date - check_in_date)
  )

# Fill missing daily_rate using room_type rack rate with rate_code discount
rack_rates <- c(STD = 189, KNG = 219, DBL = 199, JRS = 289, STE = 399)
rate_discounts <- c(BAR = 1.0, AAA = 0.85, GOV = 0.80, CORP = 0.82,
                    PKG = 0.90, DISC = 0.75)

clean_data <- clean_data %>%
  mutate(
    daily_rate = if_else(
      is.na(daily_rate),
      round(rack_rates[room_type] * rate_discounts[rate_code], 2),
      daily_rate
    )
  )
cat("Missing daily_rate imputed from rack rate * discount:",
    sum(is.na(raw_data$daily_rate)), "values\n")

# Recalculate total_revenue where missing
clean_data <- clean_data %>%
  mutate(
    total_revenue = if_else(
      is.na(total_revenue),
      round(daily_rate * nights, 2),
      total_revenue
    )
  )
cat("Missing total_revenue recalculated:",
    sum(is.na(raw_data$total_revenue)), "values\n")

# --- Step 6: Validate data ---------------------------------------------------

# Ensure no negative rates or revenue
clean_data <- clean_data %>%
  filter(daily_rate > 0, nights > 0)

cat("\nCleaned records:", nrow(clean_data), "\n\n")

# =============================================================================
# LOAD - Build Star Schema
# =============================================================================

cat("PHASE 3: LOAD (Star Schema)\n")
cat("-" |> strrep(40), "\n")

# --- dim_guest ----------------------------------------------------------------

dim_guest <- clean_data %>%
  select(guest_name, loyalty_tier, guest_type) %>%
  distinct(guest_name, .keep_all = TRUE) %>%
  mutate(guest_key = row_number()) %>%
  select(guest_key, guest_name, loyalty_tier, guest_type)

cat("dim_guest:", nrow(dim_guest), "unique guests\n")

# --- dim_room -----------------------------------------------------------------

dim_room <- tibble(
  room_key = 1:5,
  room_type_code = c("STD", "KNG", "DBL", "JRS", "STE"),
  room_type_name = c("Standard Queen", "King Room", "Double Queen",
                      "Junior Suite", "Executive Suite"),
  rack_rate = c(189, 219, 199, 289, 399),
  floor_category = c("Standard", "Standard", "Standard", "Premium", "Premium"),
  max_occupancy = c(2, 2, 4, 3, 4)
)

cat("dim_room:", nrow(dim_room), "room types\n")

# --- dim_date -----------------------------------------------------------------

date_range <- seq(ymd("2025-01-01"), ymd("2025-12-31"), by = "day")

dim_date <- tibble(
  date_key = as.integer(format(date_range, "%Y%m%d")),
  full_date = date_range,
  year = year(date_range),
  quarter = quarter(date_range),
  quarter_label = paste0("Q", quarter(date_range)),
  month = month(date_range),
  month_name = month(date_range, label = TRUE, abbr = FALSE),
  month_abbr = month(date_range, label = TRUE, abbr = TRUE),
  day = day(date_range),
  day_of_week = wday(date_range, label = TRUE),
  is_weekend = wday(date_range) %in% c(1, 7),
  season = case_when(
    month(date_range) %in% c(12, 1, 2) ~ "Winter",
    month(date_range) %in% c(3, 4, 5)  ~ "Spring",
    month(date_range) %in% c(6, 7, 8)  ~ "Summer",
    month(date_range) %in% c(9, 10, 11) ~ "Fall"
  )
)

cat("dim_date:", nrow(dim_date), "days\n")

# --- dim_channel --------------------------------------------------------------

dim_channel <- tibble(
  channel_key = 1:7,
  channel_name = c("Direct Website", "OTA-Expedia", "OTA-Booking.com",
                    "GDS", "Phone", "Walk-In", "Group"),
  channel_category = c("Direct", "OTA", "OTA", "Indirect",
                        "Direct", "Direct", "Group"),
  commission_pct = c(0.0, 0.18, 0.15, 0.10, 0.0, 0.0, 0.05)
)

cat("dim_channel:", nrow(dim_channel), "channels\n")

# --- dim_rate_code ------------------------------------------------------------

dim_rate_code <- tibble(
  rate_key = 1:6,
  rate_code = c("BAR", "AAA", "GOV", "CORP", "PKG", "DISC"),
  rate_description = c("Best Available Rate", "AAA Member Rate",
                        "Government Rate", "Corporate Negotiated",
                        "Package Rate", "Advance Discount"),
  discount_pct = c(0, 15, 20, 18, 10, 25)
)

cat("dim_rate_code:", nrow(dim_rate_code), "rate codes\n")

# --- fact_bookings ------------------------------------------------------------

# Join dimension keys to fact table
fact_bookings <- clean_data %>%
  # Join guest_key

  left_join(dim_guest %>% select(guest_key, guest_name),
            by = "guest_name") %>%
  # Join room_key
  left_join(dim_room %>% select(room_key, room_type_code),
            by = c("room_type" = "room_type_code")) %>%
  # Join date_key (check-in date)
  mutate(date_key = as.integer(format(check_in_date, "%Y%m%d"))) %>%
  # Join channel_key
  left_join(dim_channel %>% select(channel_key, channel_name),
            by = c("booking_channel" = "channel_name")) %>%
  # Join rate_key
  left_join(dim_rate_code %>% select(rate_key, rate_code),
            by = "rate_code") %>%
  # Select fact table columns
  select(
    booking_id = confirmation_no,
    guest_key,
    room_key,
    date_key,
    channel_key,
    rate_key,
    check_in_date,
    check_out_date,
    nights,
    daily_rate,
    total_revenue,
    booking_lead_days,
    num_guests,
    is_cancelled
  ) %>%
  # Add booking_id as sequential for clean surrogate key
  mutate(fact_id = row_number(), .before = booking_id)

cat("fact_bookings:", nrow(fact_bookings), "booking records\n")

# --- Export Star Schema to CSV ------------------------------------------------

cat("\nExporting star schema tables...\n")

write_csv(dim_guest, "data/processed/dim_guest.csv")
write_csv(dim_room, "data/processed/dim_room.csv")
write_csv(dim_date, "data/processed/dim_date.csv")
write_csv(dim_channel, "data/processed/dim_channel.csv")
write_csv(dim_rate_code, "data/processed/dim_rate_code.csv")
write_csv(fact_bookings, "data/processed/fact_bookings.csv")

cat("\nAll 6 tables exported to data/processed/\n")

# --- Final Summary -----------------------------------------------------------

cat("\n")
cat("=" |> strrep(60), "\n")
cat("ETL PIPELINE COMPLETE\n")
cat("=" |> strrep(60), "\n")
cat("\nStar Schema Summary:\n")
cat(sprintf("  %-20s %d rows\n", "fact_bookings", nrow(fact_bookings)))
cat(sprintf("  %-20s %d rows\n", "dim_guest", nrow(dim_guest)))
cat(sprintf("  %-20s %d rows\n", "dim_room", nrow(dim_room)))
cat(sprintf("  %-20s %d rows\n", "dim_date", nrow(dim_date)))
cat(sprintf("  %-20s %d rows\n", "dim_channel", nrow(dim_channel)))
cat(sprintf("  %-20s %d rows\n", "dim_rate_code", nrow(dim_rate_code)))
cat("\nData quality issues resolved:\n")
cat("  - Removed", n_before - n_after, "duplicate rows\n")
cat("  - Standardized mixed date formats\n")
cat("  - Normalized text casing (guest names, room types)\n")
cat("  - Unified cancellation flag encoding\n")
cat("  - Imputed missing values (daily_rate, loyalty_tier, num_guests)\n")
cat("  - Recalculated missing total_revenue\n")
