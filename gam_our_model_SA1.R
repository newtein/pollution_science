library(mgcv)

state_code <- "06"
pollutants <- c("PM2", "PM10", "O3")
lag_list <- c("0-7", "0-14", "0-21")

pollutant <- "PM2"

print(pollutant)
fname = "D:/CovidPollution/data/gam_for_our_approach.csv"
wfname = "D:/CovidPollution/r_files/our_model_summary_SA1/gam_summary"


cases_wfname = paste(wfname, pollutant, state_code, "COVID_cases.csv", sep="_")
deaths_wfname = paste(wfname, pollutant, state_code, "COVID_mortality.csv", sep="_")

df_cases <- read.csv(fname)
df_deaths <- read.csv(fname)

formula_1 = log(cases)~PM2+PM2_correlation_current_flow_betweenness+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+dayofyear+log(cases_shifted)
formula_2 = log(deaths)~PM2+PM2_correlation_current_flow_betweenness+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+dayofyear+log(deaths_shifted)

gam_y_cases <- gam(formula = formula_1, family = gaussian(), data=df_cases)
print("CASES")
sink(cases_wfname)
print(summary(gam_y_cases))
sink()

gam_y_deaths <- gam(formula = formula_2, family = gaussian(), data=df_deaths)
print("DEATHS")
sink(deaths_wfname)
print(summary(gam_y_deaths))
sink()

print(paste(pollutant, lag_t))

### PM 10
pollutant <- 'PM10'

print(pollutant)
fname = "D:/CovidPollution/data/gam_for_our_approach.csv"
wfname = "D:/CovidPollution/r_files/our_model_summary_SA1/gam_summary"


cases_wfname = paste(wfname, pollutant, state_code, "COVID_cases.csv", sep="_")
deaths_wfname = paste(wfname, pollutant, state_code, "COVID_mortality.csv", sep="_")

df_cases <- read.csv(fname)
df_deaths <- read.csv(fname)

formula_1 = log(cases)~PM10+PM10_correlation_current_flow_betweenness+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+dayofyear+log(cases_shifted)
formula_2 = log(deaths)~PM10+PM10_correlation_current_flow_betweenness+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+dayofyear+log(deaths_shifted)

gam_y_cases <- gam(formula = formula_1, family = gaussian(), data=df_cases)
print("CASES")
sink(cases_wfname)
print(summary(gam_y_cases))
sink()

gam_y_deaths <- gam(formula = formula_2, family = gaussian(), data=df_deaths)
print("DEATHS")
sink(deaths_wfname)
print(summary(gam_y_deaths))
sink()

print(paste(pollutant, lag_t))


### O3
pollutant <- 'O3'

print(pollutant)
fname = "D:/CovidPollution/data/gam_for_our_approach.csv"
wfname = "D:/CovidPollution/r_files/our_model_summary_SA1/gam_summary"


cases_wfname = paste(wfname, pollutant, state_code, "COVID_cases.csv", sep="_")
deaths_wfname = paste(wfname, pollutant, state_code, "COVID_mortality.csv", sep="_")

df_cases <- read.csv(fname)
df_deaths <- read.csv(fname)

formula_1 = log(cases)~O3+O3_correlation_current_flow_betweenness+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+dayofyear+log(cases_shifted)
formula_2 = log(deaths)~O3+O3_correlation_current_flow_betweenness+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+dayofyear+log(deaths_shifted)

gam_y_cases <- gam(formula = formula_1, family = gaussian(), data=df_cases)
print("CASES")
sink(cases_wfname)
print(summary(gam_y_cases))
sink()

gam_y_deaths <- gam(formula = formula_2, family = gaussian(), data=df_deaths)
print("DEATHS")
sink(deaths_wfname)
print(summary(gam_y_deaths))
sink()

print(pollutant)

  

