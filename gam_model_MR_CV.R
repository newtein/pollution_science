install.packages("dbplyr")
install.packages("dplyr")
install.packages('tidyverse')
loadedNamespaces()
unloadNamespace('dplyr')
library(vctrs)
library(Rcpp)
library(dplyr)
library(tidyverse)
library(modelr)
## Warning: package 'modelr' was built under R version 3.6.3
library(mgcv)


library(modelr)
library(mgcv)

state_code <- "06"
pollutants <- c("PM2")
lag_list <- c("0-7")
for (pollutant in pollutants){
  for (lag_t in lag_list){
    #print(paste(pollutant, lag_t))
    fname = "D:/CovidPollution/r_files/R_data"

    cases_fname = paste(fname, pollutant, state_code, lag_t, "COVID_cases.csv", sep="_")
    deaths_fname = paste(fname, pollutant, state_code, lag_t, "COVID_mortality.csv", sep="_")

    df_cases <- read.csv(cases_fname)
    df_deaths <- read.csv(deaths_fname)
    
    formula_1 = log(cases)~pollutant+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+date_number+log(cases_shifted)
    formula_2 = log(deaths)~pollutant+s(mean_pressure)+s(mean_rh)+s(mean_wind)+s(mean_temp)+city+date_number+log(deaths_shifted)
    
    gam_y_cases <- gam(formula = formula_1, family = gaussian(), data=df_cases)


    summary(gam_y_cases)


    #gam_y_deaths <- gam(formula = formula_2, family = gaussian(), data=df_deaths)
    #print("DEATHS")
    #sink(deaths_wfname)
    #print(summary(gam_y_deaths))
    #sink()

    #print(paste(pollutant, lag_t))
    
  }
}

