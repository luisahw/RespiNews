
#Reshape INFLCLIN and INFLWAGGR data
#Access to ZSQLCL5.ECDCDMZ.EUROPA.EU is required

reshape_INFL_data<-function(SQLite_files = T,
                                save_files = F,
                                destination_folder = getwd(),
                                current_season="2022/2023")
{
library(RODBC)
library(dplyr)
library(DBI)
library(glue)
library(fst)
library(tidyverse)

#Set-up connection to ECDC DW (ZSQLCL5.ECDCDMZ.EUROPA.EU) 
connStr <-"Driver=SQL Server;Server=ZSQLCL5.ECDCDMZ.EUROPA.EU;Database=INFL;Trusted_Connection=TRUE;"
mycon <- odbcDriverConnect(connStr)

#Reshape Denominator data within fClinicalWeeklyOverview data into long-format
ClinicalWeeklyTotals <- sqlQuery(mycon,"SELECT RecordId, SubjectCode,LocationCode, ReportingCountry, SeasonCode,
                   TimeCode, NumberOfILICases, NumberOfARICases, TotalNumberOfReportingPhysicians AS NumberOfReportingPhysicians
                          FROM [INFL].[infl].[fClinicalWeeklyOverview]
                          WHERE ValidTo is NULL",
                          stringsAsFactors = FALSE,
                           na.strings="") %>% 
            mutate(SurvType="STL") %>% 
            pivot_longer(cols = starts_with("NumberOf"),
                                 names_to = "Indicator",
                                 names_prefix = "NumberOf",
                                 values_to = "Value",
                                 values_drop_na = TRUE) 

#Reshape Qualitative data within fClinicalWeeklyOverview data into long-format
WeeklyQualitative <- sqlQuery(mycon,"SELECT RecordId, SubjectCode,LocationCode, ReportingCountry, SeasonCode,
                   TimeCode, GeographicSpreadCode AS Ind_GeographicSpread, IntensityCode AS Ind_Intensity, 
                   ImpactCode AS Ind_Impact, TrendCode AS Ind_Trend
                          FROM [INFL].[infl].[fClinicalWeeklyOverview]
                          WHERE ValidTo is NULL",
              stringsAsFactors = FALSE,
              na.strings="") %>% 
  mutate(SurvType="QUAL") %>% 
  pivot_longer(cols = starts_with("Ind_"),
               names_to = "Indicator",
               names_prefix = "Ind_",
               values_to = "Value",
               values_drop_na = TRUE) 


#Prepare fClinicalWeeklyByAge data 
ClinicalWeeklyByAge <- sqlQuery(mycon,"SELECT RecordId, SubjectCode,LocationCode, ReportingCountry, SeasonCode,
                   TimeCode, AgeClassificationCode AS Age, ClinicalSyndromeCode AS Indicator, NumberOfCases AS Value,
                   PopulationDemographyAsReported AS Denominator
                          FROM [INFL].[infl].[fClinicalWeeklyByAge]
                          WHERE ValidTo is NULL",
              stringsAsFactors = FALSE,
              na.strings="") %>% mutate(SurvType="STL") %>% 
   mutate(Age=case_when(Age=="Age05_14" ~ "05-14",
                      Age=="Age00_04" ~ "00-04",
                      Age=="Age15_64" ~ "15-64",
                      Age=="Age65+" ~ "65+",
                      Age=="AgeUnk" ~ "Unk"),
          Indicator=case_when(Indicator=="ILI" ~ "ILICases",
                              Indicator=="ARI" ~ "ARICases")) 

#Prepare VirologicalDetectionsWeekly data combining denominator and numerature data
VirologicalDetectionsWeeklyDenominators <- sqlQuery(mycon,"SELECT RecordId, SubjectCode,LocationCode, ReportingCountry, SeasonCode,
         TimeCode,SurveillanceSystemTypeCode AS SurvType,DiseaseCode, TotalNumberOfSpecimensAnalysed AS Denominator
                          FROM [INFL].[infl].[fVirologicalWeeklyOverview]
                          WHERE ValidTo is NULL",
              stringsAsFactors = FALSE,
              na.strings="")


VirologicalDetectionsWeekly <- sqlQuery(mycon,"SELECT RecordId, SubjectCode,LocationCode, ReportingCountry, SeasonCode,
         TimeCode,SurveillanceSystemTypeCode AS SurvType,DiseaseCode, PathogenTypeCode, PathogenSubtypeCode,
         NumberOfSpecimensDetected AS Value
                          FROM [INFL].[infl].[fVirologicalWeeklyByVirusType]
                          WHERE ValidTo is NULL",
              stringsAsFactors = FALSE,
              na.strings="") %>%
  full_join(.,VirologicalDetectionsWeeklyDenominators, by=c("RecordId", "SubjectCode","LocationCode", "ReportingCountry", "SeasonCode",
                      "TimeCode","SurvType","DiseaseCode"), na_matches = "never") %>% 
  mutate(SubjectCode="INFLVIR",
         PathogenTypeCode=case_when(DiseaseCode=="COVID19" ~ "COVID19",
                                    DiseaseCode=="RSV" ~ "RSV",
                                    TRUE ~ PathogenTypeCode),
         PathogenSubtypeCode=case_when(is.na(PathogenSubtypeCode) ~ "UNK",
                                       PathogenSubtypeCode=="OTH" ~ "UNK",
                                       TRUE ~PathogenSubtypeCode))

# MEM Theshold data
Thresholds_current_season <- sqlQuery(mycon,"SELECT LocationCode, SeasonCode, ThresholdCode, ClinicalSyndromeCode, ValueThreshold 
                              FROM [INFL].[common].[fThresholds] 
                               WHERE ValidTo is NULL",
                                        stringsAsFactors = FALSE,
                                        na.strings="")

current_season_thresholds <- unique(Thresholds_current_season$SeasonCode)


Thresholds <- sqlQuery(mycon,"SELECT LocationCode, SeasonCode, ThresholdCode, ClinicalSyndromeCode, ValueThreshold, ValidTo 
                              FROM [INFL].[common].[fThresholds]",
                       stringsAsFactors = FALSE,
                       na.strings="") %>% filter(SeasonCode!=current_season_thresholds) %>% 
                       group_by(SeasonCode, LocationCode, ThresholdCode, ClinicalSyndromeCode) %>% 
                       arrange(desc(ValidTo)) %>% 
                       slice(1) %>% 
                       ungroup() %>%  
                       select(-ValidTo) %>% 
                       bind_rows(Thresholds_current_season) %>% 
                       rename(Indicator=ClinicalSyndromeCode) %>% 
                       mutate(Indicator=case_when(Indicator=="ILI" ~ "ILICases",
                       Indicator=="ARI" ~ "ARICases"), SubjectCode="Threshold")
                    
                        
                  

if(SQLite_files){
  con <- dbConnect(RSQLite::SQLite(), ":memory:")
  dbListTables(con)
  dbWriteTable(con, "ClinicalWeeklyTotals", ClinicalWeeklyTotals)
  dbWriteTable(con, "WeeklyQualitative", WeeklyQualitative)
  dbWriteTable(con, "ClinicalWeeklyByAge", ClinicalWeeklyByAge)
  dbWriteTable(con, "VirologicalDetectionsWeekly", VirologicalDetectionsWeekly)
  dbWriteTable(con, "Thresholds", Thresholds)
  return(con)
}

if(!SQLite_files){
return(list(ClinicalWeeklyTotals=ClinicalWeeklyTotals,
            WeeklyQualitative=WeeklyQualitative,
            ClinicalWeeklyByAge=ClinicalWeeklyByAge,
            VirologicalDetectionsWeekly=VirologicalDetectionsWeekly,
            Thresholds=Thresholds))
}

if(save_files){
  destination_folder_final = glue('{destination_folder}/data/')
  dir.create(destination_folder_final)
  write_fst(ClinicalWeeklyTotals, glue('{destination_folder_final}/ClinicalWeeklyTotals.fst'))
  write_fst(VirologicalDetectionsWeekly, glue('{destination_folder_final}/VirologicalDetectionsWeekly.fst'))
  write_fst(WeeklyQualitative, glue('{destination_folder_final}/WeeklyQualitative.fst'))
  write_fst(ClinicalWeeklyByAge, glue('{destination_folder_final}/ClinicalWeeklyByAge.fst'))
  write_fst(Thresholds, glue('{destination_folder_final}/Thresholds.fst'))

}
}