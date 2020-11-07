library(mgcv)

state_code <- "06"
pollutants <- c("PM2", "PM10", "O3")
lag_list <- c("0-7", "0-14", "0-21")
for (pollutant in pollutants){
  for (lag_t in lag_list){
    print(paste(pollutant, lag_t))
    fname = "D:/CovidPollution/r_files/source_file_SA2/R_data"
    wfname = "D:/CovidPollution/r_files/model_summary_SA2/gam_summary"
    
    cases_fname = paste(fname, pollutant, state_code, lag_t, "COVID_cases.csv", sep="_")
    deaths_fname = paste(fname, pollutant, state_code, lag_t, "COVID_mortality.csv", sep="_")
    cases_wfname = paste(wfname, pollutant, state_code, lag_t, "COVID_cases.csv", sep="_")
    deaths_wfname = paste(wfname, pollutant, state_code, lag_t, "COVID_mortality.csv", sep="_")
    
    df_cases <- read.csv(cases_fname)
    df_deaths <- read.csv(deaths_fname)
    
    formula_1 = log(cases)~pollutant+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+date_number+log(cases_shifted)
    formula_2 = log(deaths)~pollutant+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+date_number+log(deaths_shifted)
    
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
    
  }
}

