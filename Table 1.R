#SYNDOMIC and SENTINEL data for Joint Bulletin

report_week = "2022-46"
min_date = "2022-41"

source("./load_packages.R", echo=TRUE)
source("./helper_functions.R", echo=TRUE)

# Datacheck DW and WB comparison
# Can be run to compare INFL Raw and DW, script needs to be cleaned up/not a function at the moment
# source('./INFCLIN_check.R', echo=TRUE)

#If run like this it creates an SQLlite dataset with data from INFLCLIN and INFLAGGR
#The following datasets are included:
#1."ClinicalWeeklyDenominators"
#2."WeeklyQualitative"
#3."ClinicalWeeklyByAge"
#4."VirologicalDetectionsWeekly"
#5."MEMThresholds"

source(file="./INFL_DW_conversion.R")
con <- reshape_INFL_data(SQLite_files = T)

#Creates a reactable Table with ILI/ARI rates including trends AND
#Positivity,Trend and number tested for COVID19, INFL and RSV

source(file="./SummaryTable STL.R")

#Current issues: Filtering all thresholds in the dataset (need to apply rule to ValidTo)
# This means that at the moment this only works for most current season
# Lack of reporting during the season means that trends as well as thresholds are hard 
# to set between seasons. Best to restrict this to seasons or change the approach. 
# Thresholds pre-pandemic vs post-pandemic are currently not being filtered. needs to be decided.
Table1 <- sparkline_table(max_date = report_week_W, min_date= min_date_W, included_countries=EUnames)

Table1

