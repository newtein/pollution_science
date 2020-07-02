library(mgcv)
df <- read.csv("D:/CovidPollution/r_files/pollutant_pm2.csv")
lm(cases~pollutant+mean_pressure+mean_rh+mean_wind+city+date_number+cases_shifted, data=df)
gam_y <- gam(log(cases)~pollutant+s(mean_pressure)+s(mean_rh)+s(mean_wind)+city+date_number+log(cases_shifted), method = "REML", data=df)
gam_y
summary(gam_y)
