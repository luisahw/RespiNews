#Compare data in DW with data in the INFL Raw dataset

INFLCLIN <- read_csv("C:/Users/lhallmaierwacker/Downloads/INFLCLIN.csv")

connStr <-"Driver=SQL Server;Server=ZSQLCL5.ECDCDMZ.EUROPA.EU;Database=INFL;Trusted_Connection=TRUE;"
mycon <- odbcDriverConnect(connStr)

ClinicalWeeklyByAge <- sqlQuery(mycon,"SELECT * FROM [INFL].[infl].[fClinicalWeeklyByAge]
                          WHERE ValidTo is NULL",
                                stringsAsFactors = FALSE,
                                na.strings="")

INFLCLIN_ByAge <- INFLCLIN %>% select(RecordId, ReportingCountry,DataSource, TimeCode,
                                      Season, `ILI00-04`, `ILI05-14`, `ILI15-64`, `ILI65+`, ILIUnk,
                                      `ILI_Denominator00-04`, `ILI_Denominator05-14`, `ILI_Denominator15-64`,
                                      `ILI_Denominator65+`, ILI_DenominatorUnk, `ARI_Denominator00-04`, `ARI_Denominator05-14`,
                                      `ARI_Denominator15-64`, `ARI_Denominator65+`, ARI_DenominatorUnk, `ARI00-04`, `ARI05-14`,
                                      `ARI15-64`, `ARI65+`, ARIUnk)

###ILI long format----
INFLCLIN_ByAge_ILI <- INFLCLIN_ByAge %>% select(RecordId, ReportingCountry,DataSource, TimeCode,
                                                Season, `ILI00-04`, `ILI05-14`, `ILI15-64`, `ILI65+`, ILIUnk) %>%
                                          mutate(`ILI15-64`=as.numeric(`ILI15-64`)) %>%
                                          pivot_longer(cols = starts_with("ILI"),
                                                       names_to = "AgeClassificationCode",
                                                       names_prefix = "ILI",
                                                       values_to = "NumberofCases",
                                                       values_drop_na = TRUE) %>%
                                          mutate(ClinicalSyndromeCode="ILI")


INFLCLIN_ByAge_ILIDenom <- INFLCLIN_ByAge %>% select(RecordId, ReportingCountry,DataSource, TimeCode,
                                                Season, `ILI_Denominator00-04`, `ILI_Denominator05-14`, `ILI_Denominator15-64`,
                                                `ILI_Denominator65+`, ILI_DenominatorUnk) %>%
                                                  pivot_longer(cols = starts_with("ILI"),
                                                               names_to = "AgeClassificationCode",
                                                               names_prefix = "ILI_Denominator",
                                                               values_to = "PopulationDemographyAsReported",
                                                               values_drop_na = TRUE) %>%
                                                  mutate(ClinicalSyndromeCode="ILI") %>%
                                                  select(RecordId,AgeClassificationCode,PopulationDemographyAsReported, TimeCode)

INFLCLIN_ByAge_ILI_Comb <- full_join(INFLCLIN_ByAge_ILI, INFLCLIN_ByAge_ILIDenom, by=c("RecordId", "AgeClassificationCode", "TimeCode"))
rm(INFLCLIN_ByAge_ILI, INFLCLIN_ByAge_ILIDenom)

###ARI long format----
INFLCLIN_ByAge_ARI <- INFLCLIN_ByAge %>% select(RecordId, ReportingCountry,DataSource, TimeCode,
                                                Season, `ARI00-04`, `ARI05-14`, `ARI15-64`, `ARI65+`, ARIUnk) %>%
  pivot_longer(cols = starts_with("ARI"),
               names_to = "AgeClassificationCode",
               names_prefix = "ARI",
               values_to = "NumberofCases",
               values_drop_na = TRUE) %>%
  mutate(ClinicalSyndromeCode="ARI")


INFLCLIN_ByAge_ARIDenom <- INFLCLIN_ByAge %>% select(RecordId, ReportingCountry,DataSource, TimeCode,
                                                     Season, `ARI_Denominator00-04`, `ARI_Denominator05-14`, `ARI_Denominator15-64`,
                                                     `ARI_Denominator65+`, ARI_DenominatorUnk) %>%
  pivot_longer(cols = starts_with("ARI"),
               names_to = "AgeClassificationCode",
               names_prefix = "ARI_Denominator",
               values_to = "PopulationDemographyAsReported",
               values_drop_na = TRUE) %>%
  mutate(ClinicalSyndromeCode="ARI") %>%
  select(RecordId,AgeClassificationCode,PopulationDemographyAsReported, TimeCode)

INFLCLIN_ByAge_ARI_Comb <- full_join(INFLCLIN_ByAge_ARI,
                                     INFLCLIN_ByAge_ARIDenom,
                                     by=c("RecordId", "AgeClassificationCode",
                                          "TimeCode"))

INFLCLIN_ByAge_Comb <- bind_rows(INFLCLIN_ByAge_ARI_Comb, INFLCLIN_ByAge_ILI_Comb)%>%
                        filter(!is.na(ReportingCountry))
rm(INFLCLIN_ByAge_ARI, INFLCLIN_ByAge_ARIDenom)

#ClinicalWeeklyByAge adjust Age-Clasificiation to match that in WB to join datasets
ClinicalWeeklyByAge <- ClinicalWeeklyByAge %>% mutate(AgeClassificationCode=case_when(AgeClassificationCode=="Age05_14" ~ "05-14",
                                                                    AgeClassificationCode=="Age00_04" ~ "00-04",
                                                                    AgeClassificationCode=="Age15_64" ~ "15-64",
                                                                    AgeClassificationCode=="Age65+" ~ "65+",
                                                                    AgeClassificationCode=="AgeUnk" ~ "Unk")) %>% filter(is.na(ValidTo))

INFLCLIN_ClinicalWeeklyByAge_Comb <- left_join(INFLCLIN_ByAge_Comb,
                                               ClinicalWeeklyByAge,
                                               by=c("AgeClassificationCode",
                                                    "TimeCode", "ClinicalSyndromeCode", "ReportingCountry"))

Calc <- INFLCLIN_ClinicalWeeklyByAge_Comb %>% mutate(Case_dif=NumberofCases-NumberOfCases,
                                                     Denom_dif=PopulationDemographyAsReported.y-PopulationDemographyAsReported.x)

#There is an issue with the UK data because it is already split into 4 nations in
#the DW data but not in the WB data. Therefore removing all UK data for the timebeing
Calc_Cases <- Calc %>% filter(Case_dif!=0) %>% filter(ReportingCountry!="UK") %>%
                        select(RecordId.x, RecordId.y, ClinicalWeeklyByAgeId,
                               ReportingCountry, TimeCode, Season,
                               AgeClassificationCode,ClinicalSyndromeCode,
                               NumberofCases, NumberOfCases,
                               PopulationDemographyAsReported.x,
                               PopulationDemographyAsReported.y,ValidFrom,ValidTo) %>%
                        rename(NumberOfCases_WB=NumberofCases,
                               NumberOfCases_DW=NumberOfCases,
                               PopulationDemographyAsReported.WB=PopulationDemographyAsReported.x,
                               PopulationDemographyAsReported.DW= PopulationDemographyAsReported.y)

Denom_Cases <- Calc %>% filter(Denom_dif!=0) %>% filter(ReportingCountry!="UK") %>%
  select(RecordId.x, RecordId.y, ClinicalWeeklyByAgeId,
         ReportingCountry, TimeCode, Season,
         AgeClassificationCode,ClinicalSyndromeCode,
         NumberofCases, NumberOfCases,
         PopulationDemographyAsReported.x,
         PopulationDemographyAsReported.y,ValidFrom,ValidTo) %>%
  rename(NumberOfCases_WB=NumberofCases,
         NumberOfCases_DW=NumberOfCases,
         PopulationDemographyAsReported.WB=PopulationDemographyAsReported.x,
         PopulationDemographyAsReported.DW= PopulationDemographyAsReported.y)
