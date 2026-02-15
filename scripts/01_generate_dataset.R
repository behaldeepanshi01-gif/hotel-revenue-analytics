# =============================================================================
# 01_generate_dataset.R
# Hotel Revenue Analytics Pipeline - Dataset Generation
#
# Generates a simulated PMS export (OnQ/OPERA style) for a 250-room hotel
# in Washington D.C. covering Jan-Dec 2025.
# Produces 1,800 rows of deliberately messy data to demonstrate ETL skills.
# =============================================================================

library(tidyverse)
library(lubridate)

set.seed(515)

# --- Configuration -----------------------------------------------------------

n_bookings <- 1800
hotel_capacity <- 250

# Room types with rack rates (industry-standard codes)
room_types <- tibble(
  room_type = c("STD", "KNG", "DBL", "JRS", "STE"),
  rack_rate = c(189, 219, 199, 289, 399),
  description = c("Standard Queen", "King Room", "Double Queen",
                   "Junior Suite", "Executive Suite")
)

# Rate codes (common in PMS systems)
rate_codes <- c("BAR", "AAA", "GOV", "CORP", "PKG", "DISC")
rate_discounts <- c(BAR = 1.0, AAA = 0.85, GOV = 0.80, CORP = 0.82,
                    PKG = 0.90, DISC = 0.75)

# Booking channels
channels <- c("Direct Website", "OTA-Expedia", "OTA-Booking.com",
              "GDS", "Phone", "Walk-In", "Group")
channel_weights <- c(0.20, 0.18, 0.15, 0.12, 0.15, 0.08, 0.12)

# Loyalty tiers (Marriott Bonvoy style)
loyalty_tiers <- c("Diamond", "Gold", "Silver", "Blue", "None")
loyalty_weights <- c(0.08, 0.15, 0.20, 0.25, 0.32)

# --- Generate Base Bookings --------------------------------------------------

cat("Generating", n_bookings, "hotel booking records...\n")

# Generate check-in dates across 2025, with seasonal weighting
# Higher volume in spring (cherry blossom) and fall (convention season)
month_weights <- c(0.06, 0.07, 0.09, 0.11, 0.10, 0.08,
                   0.07, 0.08, 0.09, 0.10, 0.09, 0.06)
booking_months <- sample(1:12, n_bookings, replace = TRUE, prob = month_weights)
booking_days <- sapply(booking_months, function(m) {
  max_day <- days_in_month(ymd(paste0("2025-", m, "-01")))
  sample(1:max_day, 1)
})
check_in_dates <- ymd(paste("2025", booking_months, booking_days, sep = "-"))

# Length of stay (1-7 nights, weighted toward 1-3)
los <- sample(1:7, n_bookings, replace = TRUE,
              prob = c(0.25, 0.30, 0.20, 0.10, 0.07, 0.05, 0.03))
check_out_dates <- check_in_dates + days(los)

# Room types
room_type_sample <- sample(room_types$room_type, n_bookings, replace = TRUE,
                           prob = c(0.30, 0.25, 0.25, 0.12, 0.08))

# Rate codes
rate_code_sample <- sample(rate_codes, n_bookings, replace = TRUE,
                           prob = c(0.35, 0.12, 0.10, 0.18, 0.15, 0.10))

# Calculate daily rate based on room type rack rate and rate code discount
daily_rates <- sapply(1:n_bookings, function(i) {
  rack <- room_types$rack_rate[room_types$room_type == room_type_sample[i]]
  discount <- rate_discounts[rate_code_sample[i]]
  # Add some random variation (+/- 10%)
  base_rate <- rack * discount
  round(base_rate * runif(1, 0.90, 1.10), 2)
})

# Total revenue
total_revenue <- round(daily_rates * los, 2)

# Channels
channel_sample <- sample(channels, n_bookings, replace = TRUE,
                         prob = channel_weights)

# Loyalty tiers
loyalty_sample <- sample(loyalty_tiers, n_bookings, replace = TRUE,
                         prob = loyalty_weights)

# Guest names (generate ~350 unique guests, some repeat)
first_names <- c("James", "Mary", "Robert", "Patricia", "John", "Jennifer",
                 "Michael", "Linda", "David", "Elizabeth", "William", "Barbara",
                 "Richard", "Susan", "Joseph", "Jessica", "Thomas", "Sarah",
                 "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
                 "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra",
                 "Deepanshi", "Priya", "Raj", "Anita", "Sanjay", "Meera",
                 "Carlos", "Maria", "Wei", "Yuki", "Ahmed", "Fatima",
                 "Olga", "Dmitri", "Hans", "Sofia", "Pierre", "Aiko",
                 "Kwame", "Amara", "Chen", "Liam", "Emma", "Noah", "Olivia",
                 "Ava", "Ethan", "Sophia", "Mason", "Isabella")
last_names <- c("Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia",
                "Miller", "Davis", "Rodriguez", "Martinez", "Hernandez",
                "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor",
                "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
                "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis",
                "Patel", "Sharma", "Gupta", "Singh", "Kumar", "Chen", "Wang",
                "Kim", "Park", "Tanaka", "Suzuki", "Muller", "Schmidt",
                "Dubois", "Laurent", "Okafor", "Mensah", "Ali", "Khan")

n_unique_guests <- 350
guest_pool <- paste(
  sample(first_names, n_unique_guests, replace = TRUE),
  sample(last_names, n_unique_guests, replace = TRUE)
)
guest_names <- sample(guest_pool, n_bookings, replace = TRUE)

# Confirmation numbers (8-digit)
conf_numbers <- paste0("CF", sprintf("%08d", sample(10000000:99999999, n_bookings)))

# Cancellation flag (~12% cancellation rate, higher for OTAs)
cancel_prob <- ifelse(grepl("OTA", channel_sample), 0.18,
               ifelse(channel_sample == "Direct Website", 0.08, 0.10))
cancellation <- rbinom(n_bookings, 1, cancel_prob)

# Guest type
guest_type <- ifelse(channel_sample == "Group", "Group", "Transient")

# Booking lead days (days between booking and check-in)
lead_days <- pmax(0, round(rnorm(n_bookings, mean = 30, sd = 20)))
lead_days[channel_sample == "Walk-In"] <- 0

# Number of guests
num_guests <- sample(1:4, n_bookings, replace = TRUE,
                     prob = c(0.35, 0.40, 0.15, 0.10))

# --- Introduce Messiness (for ETL demonstration) -----------------------------

cat("Introducing data quality issues...\n")

# 1. Mixed date formats (~30% use MM/DD/YYYY, rest use YYYY-MM-DD)
format_mask <- sample(c(TRUE, FALSE), n_bookings, replace = TRUE,
                      prob = c(0.70, 0.30))
check_in_str <- ifelse(format_mask,
                       format(check_in_dates, "%Y-%m-%d"),
                       format(check_in_dates, "%m/%d/%Y"))
check_out_str <- ifelse(format_mask,
                        format(check_out_dates, "%Y-%m-%d"),
                        format(check_out_dates, "%m/%d/%Y"))

# 2. Inconsistent text casing for guest names
case_mask <- sample(1:3, n_bookings, replace = TRUE, prob = c(0.60, 0.25, 0.15))
guest_names_messy <- case_when(
  case_mask == 1 ~ guest_names,                    # Title Case (correct)
  case_mask == 2 ~ toupper(guest_names),            # ALL CAPS
  case_mask == 3 ~ tolower(guest_names)             # all lowercase
)

# 3. Inconsistent room type casing
room_case <- sample(1:3, n_bookings, replace = TRUE, prob = c(0.70, 0.15, 0.15))
room_type_messy <- case_when(
  room_case == 1 ~ room_type_sample,
  room_case == 2 ~ tolower(room_type_sample),
  room_case == 3 ~ paste0(substr(room_type_sample, 1, 1),
                           tolower(substr(room_type_sample, 2, 3)))
)

# 4. Mixed Y/N encoding for cancellation
cancel_encoding <- sample(1:4, n_bookings, replace = TRUE,
                          prob = c(0.40, 0.25, 0.20, 0.15))
cancel_str <- case_when(
  cancel_encoding == 1 & cancellation == 1 ~ "Y",
  cancel_encoding == 1 & cancellation == 0 ~ "N",
  cancel_encoding == 2 & cancellation == 1 ~ "Yes",
  cancel_encoding == 2 & cancellation == 0 ~ "No",
  cancel_encoding == 3 & cancellation == 1 ~ "yes",
  cancel_encoding == 3 & cancellation == 0 ~ "no",
  cancel_encoding == 4 & cancellation == 1 ~ "1",
  cancel_encoding == 4 & cancellation == 0 ~ "0"
)

# 5. Missing values (~3% of daily_rate, ~5% of loyalty_tier, ~2% of num_guests)
daily_rates_messy <- daily_rates
daily_rates_messy[sample(n_bookings, round(n_bookings * 0.03))] <- NA

loyalty_messy <- loyalty_sample
loyalty_messy[sample(n_bookings, round(n_bookings * 0.05))] <- NA

num_guests_messy <- num_guests
num_guests_messy[sample(n_bookings, round(n_bookings * 0.02))] <- NA

# 6. Add whitespace to some channel names
channel_messy <- channel_sample
ws_idx <- sample(n_bookings, round(n_bookings * 0.10))
channel_messy[ws_idx] <- paste0("  ", channel_messy[ws_idx], " ")

# 7. Some total_revenue values are missing (will need recalculation)
total_rev_messy <- total_revenue
total_rev_messy[sample(n_bookings, round(n_bookings * 0.04))] <- NA

# --- Assemble DataFrame ------------------------------------------------------

bookings_raw <- tibble(
  confirmation_no = conf_numbers,
  guest_name = guest_names_messy,
  loyalty_tier = loyalty_messy,
  room_type = room_type_messy,
  rate_code = rate_code_sample,
  check_in_date = check_in_str,
  check_out_date = check_out_str,
  nights = los,
  daily_rate = daily_rates_messy,
  total_revenue = total_rev_messy,
  booking_channel = channel_messy,
  cancellation_flag = cancel_str,
  guest_type = guest_type,
  num_guests = num_guests_messy,
  booking_lead_days = lead_days
)

# 8. Add ~30 duplicate rows (exact duplicates to be removed in ETL)
dup_indices <- sample(n_bookings, 30)
bookings_with_dups <- bind_rows(bookings_raw, bookings_raw[dup_indices, ])

# Shuffle the rows
bookings_final <- bookings_with_dups[sample(nrow(bookings_with_dups)), ]

# --- Export -------------------------------------------------------------------

output_path <- "data/raw/hotel_bookings_raw.csv"
write_csv(bookings_final, output_path)

cat("\nDataset generated successfully!\n")
cat("Total rows (with duplicates):", nrow(bookings_final), "\n")
cat("Unique bookings:", n_bookings, "\n")
cat("Duplicate rows added: 30\n")
cat("Output:", output_path, "\n")

# Summary of data quality issues introduced:
cat("\n--- Data Quality Issues ---\n")
cat("1. Mixed date formats (YYYY-MM-DD and MM/DD/YYYY)\n")
cat("2. Inconsistent guest name casing (Title, UPPER, lower)\n")
cat("3. Inconsistent room type casing\n")
cat("4. Mixed cancellation encoding (Y/N, Yes/No, yes/no, 1/0)\n")
cat("5. Missing values in daily_rate, loyalty_tier, num_guests, total_revenue\n")
cat("6. Leading/trailing whitespace in booking_channel\n")
cat("7. 30 duplicate rows\n")
