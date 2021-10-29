library(tidyverse)
library(lubridate)
library(epiextractr)
library(vroom)

# JOLTS data from Elise
jolts_data <- read_csv("jolts.csv") %>%
  mutate(state_fips = as.numeric(`State ID`)) %>% 
  mutate(name = case_when(
    str_detect(`Series name`, "levels, seas") ~ "level_sa",
    str_detect(`Series name`, "levels, not seas") ~ "level_nsa",
    str_detect(`Series name`, "rate, seas") ~ "rate_sa",
    str_detect(`Series name`, "rate, not seas") ~ "rate_nsa"
  )) %>% 
  pivot_longer(matches("2020|2021"), names_to = "date") %>% 
  pivot_wider(state_fips|date, names_prefix = "quit_") %>% 
  mutate(date = str_replace(date, "\\n", " ")) %>% 
  mutate(year = year(my(date)), month = month(my(date))) %>% 
  select(-date) %>% 
  filter(state_fips != 0)

# CES SM data
download_sm <- function(x) {
  system(paste0(
    "wget -q -N https://download.bls.gov/pub/time.series/sm/sm.",
    x
  ))
  vroom(paste0("sm.", x))
}

ces_data <- download_sm("data.0.Current") %>% 
  filter(year >= 2019) %>% 
  select(series_id, year, period, value) %>% 
  # merge series information
  inner_join(download_sm("series"), by = "series_id") %>% 
  mutate(
    state_fips = as.numeric(state_code),
    month = as.numeric(str_sub(period, 2, 3)),
    name = case_when(
      industry_code == "05000000" & seasonal == "S" ~ "private_sa",
      industry_code == "05000000" & seasonal == "U" ~ "private_nsa",
      industry_code == "00000000" & seasonal == "S" ~ "overall_sa",
      industry_code == "00000000" & seasonal == "U" ~ "overall_nsa",
    )
  ) %>% 
  # keep only states, monthly data
  filter(
    area_code == "00000" & state_fips >= 1 & state_fips <= 56,
    month <= 12 & year >= 2020,
    data_type_code == "01",
    !is.na(name)
  ) %>% 
  pivot_wider(state_fips|year|month, names_prefix = "emp_")

state_codes <- load_org(2021, statefips) %>% 
  count(statefips) %>% 
  transmute(
    state_abb = as.character(haven::as_factor(statefips)),
    state_fips = statefips
  )

basic_data <- load_basic(2020:2021, year, month, statefips, lfstat, age, basicwgt) %>% 
  filter(basicwgt > 0 & age >= 16) %>% 
  mutate(
    epop_all = lfstat == 1,
    epop_prime = ifelse(age >= 25 & age <= 54, epop_all, NA),
    urate_all = case_when(lfstat == 1 ~ 0, lfstat == 2 ~ 1)
  ) %>% 
  group_by(state_fips = statefips, year, month) %>% 
  summarize(
    across(epop_all|urate_all|epop_prime, ~ weighted.mean(.x, w = basicwgt, na.rm = TRUE))
  ) %>% 
  ungroup()

pop_data <- vroom(fs::dir_ls(path = "data_pop/", glob = "*SC*.csv")) %>% 
  filter(YEAR == 12) %>% 
  transmute(
    state_fips = as.numeric(STATE),
    pop_1864 = AGE18PLUS_TOT - AGE65PLUS_TOT,
    pop_total = POPESTIMATE,
    pop_1824_share = AGE1824_TOT / pop_total,
    pop_2544_share = AGE2544_TOT / pop_total,
    pop_4564_share = AGE4564_TOT / pop_total,
    pop_65up_share = AGE65PLUS_TOT / pop_total
  )

cases_deaths_data <- read_csv("us-states.csv") %>% 
  filter(date <= ym("2021-09")) %>% 
  mutate(
    state_fips = as.numeric(fips),
    month = month(date), 
    year = year(date)
  ) %>% 
  inner_join(pop_data, by = "state_fips") %>% 
  rename(cases_level = cases, deaths_level = deaths) %>% 
  mutate(cases_rate = cases_level / pop_total, deaths_rate = deaths_level / pop_total) %>% 
  group_by(state_fips, year, month) %>% 
  summarize(across(matches("cases|deaths"), mean)) %>% 
  ungroup()

vax_data <- read_csv("cdc_vax.csv") %>% 
  mutate(
    date = mdy(Date), 
    month = month(date), 
    year = year(date)
  ) %>% 
  filter(date <= ym("2021-09")) %>% 
  mutate(
    fully_vax_18plus = Series_Complete_18Plus,
    fully_vax_65plus = Series_Complete_65Plus,
    fully_vax_1864 = fully_vax_18plus - fully_vax_65plus,
  ) %>% 
  rename(state_abb = Location) %>% 
  inner_join(state_codes, by = "state_abb") %>% 
  inner_join(pop_data, by = "state_fips") %>% 
  mutate(
    vax_rate = Series_Complete_Pop_Pct,
    vax_1864_rate = fully_vax_1864 / pop_1864 * 100
  ) %>% 
  group_by(state_fips, year, month) %>% 
  summarize(across(vax_rate|vax_1864_rate, mean)) %>% 
  ungroup()

vax_cases_emp <- list(basic_data, 
                      ces_data, 
                      cases_deaths_data, 
                      vax_data,
                      jolts_data
                      ) %>% 
  reduce(full_join, by = c("state_fips", "year", "month")) %>% 
  mutate(state_abb = as.character(haven::as_factor(state_fips))) %>% 
  relocate(state_fips, state_abb, year, month)

vax_cases_emp %>% 
  write_csv("vax_cases_emp.csv")

