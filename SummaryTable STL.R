sparkline_table  <- function(html_version = T,
                             max_date = report_week_W,
                             min_date= min_date_W,
                             included_countries=EUnames){
  

  season_max_date <- agg_weeks_lookup %>% filter(year_week_W==max_date) %>% select(SeasonCode) %>% as.character()
  min_date_rep <-agg_weeks_lookup %>% filter(year_week_W==min_date)
  max_date_rep <-agg_weeks_lookup %>% filter(year_week_W==max_date)
  
  
  list_weeks_included <- agg_weeks_lookup %>% filter(date_rep<=max_date_rep$date_rep, date_rep>=min_date_rep$date_rep) 
  
  dbListTables(con)

  ## 3. Nested dataframe with sparkline html for each indicator/country  ------------
  
  
  ClinicalWeeklyByAge <- dbSendQuery(con, "SELECT RecordId, LocationCode, SeasonCode,
                                                  TimeCode, Age, Indicator, Value, Denominator
                                           FROM ClinicalWeeklyByAge")
  x<-dbFetch(ClinicalWeeklyByAge)
  
  ILI_ARI <- x %>% 
    filter(TimeCode %in% list_weeks_included$year_week_W) %>% 
    group_by(RecordId, LocationCode, TimeCode, Indicator, SeasonCode) %>% 
    nest() %>%
    mutate(data = map(data, ~ .x %>% adorn_totals("row", name = "Total"))) %>%
    unnest(cols = c(data)) %>% 
    filter(Age=="Total") %>% 
    mutate(Value=as.integer(Value*100000/(Denominator))) %>% 
    ungroup() 
  
  Tresholds <- dbSendQuery(con, "SELECT * FROM Thresholds")
  y<-dbFetch(Tresholds)
  
  
  Thresholds<-y %>% filter(SeasonCode==season_max_date) %>% 
                left_join(ILI_ARI, by=c("Indicator", "LocationCode", "SeasonCode")) %>% 
    filter(TimeCode==max_date) %>% 
    mutate(Over_threshold=Value>=ValueThreshold, Under_threshold=Value<ValueThreshold) %>% 
    group_by(LocationCode, Indicator) %>% 
    filter(Over_threshold) %>% 
    arrange(desc(ValueThreshold)) %>% 
    slice(1) %>% 
    ungroup() %>% 
    select(LocationCode, ValueThreshold, ThresholdCode,Indicator) %>% 
    pivot_wider(names_from = Indicator, values_from = c(ThresholdCode,ValueThreshold))
    
  ILI_ARI <- ILI_ARI %>%
    select(LocationCode, TimeCode, Indicator, Value) %>% 
    pivot_wider(names_from = Indicator, values_from = Value)
  
  VirologicalDetectionsWeekly <- dbSendQuery(con, "SELECT RecordId, LocationCode, SeasonCode,
                                                  TimeCode, DiseaseCode, PathogenSubtypeCode, Value, Denominator
                                           FROM VirologicalDetectionsWeekly
                                             WHERE SurvType='STL'")
  x<-dbFetch(VirologicalDetectionsWeekly)
  
    PositivityRate <- x  %>% filter(TimeCode %in% list_weeks_included$year_week_W) %>% 
    group_by(RecordId, LocationCode, TimeCode, DiseaseCode, SeasonCode, Denominator) %>% 
    nest() %>%
    mutate(data = map(data, ~ .x %>% adorn_totals("row", name = "Total"))) %>%
    unnest(cols = c(data)) %>% 
    filter(PathogenSubtypeCode=="Total") %>% 
    ungroup() %>% 
    mutate(Pos_percentage=as.integer((Value/Denominator)*100)) %>% 
    pivot_wider(names_from = DiseaseCode, values_from = c(Denominator, Value, Pos_percentage)) %>% 
    select(-RecordId, -SeasonCode, -PathogenSubtypeCode)
  

    
latest <- full_join(ILI_ARI,PositivityRate, by=c("LocationCode", "TimeCode")) %>% 
      group_by(LocationCode) %>% 
      filter(TimeCode==max_date) %>% 
      full_join(Thresholds, by= "LocationCode") %>% 
      mutate(ThresholdCode_ILICases=case_when(ThresholdCode_ILICases=="PreEpidemic" ~ "Epidemic threshold",
                                         ThresholdCode_ILICases=="PostEpidemic"~ "Epidemic threshold",
                                         ThresholdCode_ILICases=="CI50" ~ "medium",
                                         ThresholdCode_ILICases=="CI90" ~ "high",
                                         ThresholdCode_ILICases=="CI95" ~ "high",
                                    TRUE ~ "--"),
             ThresholdCode_ARICases=case_when(ThresholdCode_ARICases=="PreEpidemic" ~ "Epidemic threshold",
                                     ThresholdCode_ARICases=="PostEpidemic"~ "Epidemic threshold",
                                     ThresholdCode_ARICases=="CI50" ~ "medium",
                                     ThresholdCode_ARICases=="CI90" ~ "high",
                                     ThresholdCode_ARICases=="CI95" ~ "high",
                                 TRUE ~ "--")) 



data_spark_line <- full_join(ILI_ARI,PositivityRate, by=c("LocationCode", "TimeCode")) %>% 
                   select(LocationCode, TimeCode, ILICases, ARICases, Pos_percentage_INFL, Pos_percentage_RSV, Pos_percentage_COVID19) %>% 
                   right_join(select(CountryCode, c(LocationCode)), by = "LocationCode") %>% 
                   group_by(LocationCode) %>% 
                   nest() %>% 
                  mutate(data = map(data, ~
                            .x %>%  # join to weeks look up to complete time series with NA for missing weeks
                            right_join(select(list_weeks_included, c(year_week_W, date_rep)), by = c("TimeCode"="year_week_W")) %>% 
                              arrange(date_rep))) %>%
                  unnest(cols = c(data)) %>% 
                  summarise(across(!starts_with("LocationCode"), ~list((.)))) %>% 
                  rename(ILI_cases=ILICases, ARI_cases=ARICases,INFL=Pos_percentage_INFL,RSV=Pos_percentage_RSV, COVID19=Pos_percentage_COVID19) %>% 
                  ungroup() %>% full_join(latest, by=c("LocationCode"))%>%
                  mutate(ThresholdCode_ILICases=if_else(is.na(ThresholdCode_ILICases), "--", ThresholdCode_ILICases)) %>% 
                  mutate(ThresholdCode_ARICases=if_else(is.na(ThresholdCode_ARICases), "--", ThresholdCode_ARICases)) %>% 
                  left_join(select(CountryCode, c(LocationCode, CountryName)), by = "LocationCode") %>%  
                  filter(LocationCode %in% included_countries) %>% 
                  select(CountryName, ARI_cases, ThresholdCode_ARICases, ILI_cases,ThresholdCode_ILICases, Pos_percentage_COVID19, COVID19, Denominator_COVID19,
                         Pos_percentage_INFL, INFL, Denominator_INFL, Pos_percentage_RSV, RSV, Denominator_RSV)



  
  
obj = reactable(data_spark_line,
                    sortable = FALSE,
                     filterable = TRUE,
                    showSortable = TRUE,
                striped = TRUE,
                style = list(fontFamily = "Calibri"),
                pagination = FALSE,
                highlight = TRUE,
                height = 700, 
                width = "100%",
                    theme = reactableTheme(
                      headerStyle = list(
                        'font-size' = '90%',
                        'background-color' = '#69ae23',
                        'color' = '#fff')
                    ),
                    #defaultColDef = colDef(show = F),
                    columns = list(
                    CountryName =colDef(name = "Country Name", sticky = "left",style = list(borderLeft = "1px solid #eee", maxWidth = 90),
                    headerStyle = list(borderLeft = "1px solid #eee"), sortable = TRUE),
                    ARI_cases=colDef(cell=function (ARI_cases, index){
                      sparkline(data_spark_line$ARI_cases[[index]], type = "line", tooltipFormat = "{{y.1}}",
                                lineColor ="grey", fillColor = NA,
                                lineWidth = 2, minSpotColor = F, maxSpotColor = F)
                    }
                    # ,
                    # name = "ARI"
                    ),
                    ThresholdCode_ARICases=colDef(style = function(value, index) {
                      color <- if (data_spark_line$ThresholdCode_ARICases[[index]] == "Epidemic threshold") {
                        "#DC9635"
                      } else if (data_spark_line$ThresholdCode_ARICases[[index]] == "medium") {
                        "#b45f06"
                      }else if (data_spark_line$ThresholdCode_ARICases[[index]] == "high") {
                        "#7C170F"
                      }
                      else{
                        color <- "#808080"
                      }
                      list(color = color, fontWeight = "bold")
                    },sortable = TRUE, name = "MEM"),
                    ILI_cases=colDef(cell=function (ILI_cases, index){
                    sparkline(data_spark_line$ILI_cases[[index]], type = "line", tooltipFormat = "{{y.1}}", 
                              lineColor ="grey", fillColor = NA, 
                              lineWidth = 2, minSpotColor = F, maxSpotColor = F)
                      },name = "ILI"),
                    ThresholdCode_ILICases=colDef(style = function(value, index) {
                      color <- if (data_spark_line$ThresholdCode_ILICases[[index]] == "Epidemic threshold") {
                        "#DC9635"
                      } else if (data_spark_line$ThresholdCode_ILICases[[index]] == "medium") {
                        "#b45f06"
                      }else if (data_spark_line$ThresholdCode_ILICases[[index]] == "high") {
                        "#7C170F"
                      }
                      else{
                        color <- "#808080"
                      }
                      list(color = color, fontWeight = "bold")
                      },sortable = TRUE, name = "MEM"),
                    Pos_percentage_COVID19 = colDef(name = "%", format=colFormat(suffix= " %"),sortable = TRUE),
                    COVID19=colDef(cell=function (COVID19, index){
                      sparkline(data_spark_line$COVID19[[index]], type = "line", tooltipFormat = "{{y.1}}", 
                                lineColor ="grey", fillColor = NA, 
                                lineWidth = 2, minSpotColor = F, maxSpotColor = F)
                    },name = "Trend"),
                    Denominator_COVID19 = colDef(name = "# tested", sortable = TRUE),
                   
                    
                    Pos_percentage_INFL = colDef(name = "%", format=colFormat(suffix= " %"),sortable = TRUE),
                    INFL=colDef(cell=function (INFL, index){
                      sparkline(data_spark_line$INFL[[index]], type = "line", tooltipFormat = "{{y.1}}", 
                                lineColor ="grey", fillColor = NA, 
                                lineWidth = 2, minSpotColor = F, maxSpotColor = F)
                    },name = "Trend"),
                    Denominator_INFL = colDef(name = "# tested", sortable = TRUE),
                    
                    Pos_percentage_RSV = colDef(name = "%", format=colFormat(suffix= " %"),sortable = TRUE),
                    RSV=colDef(cell=function (RSV, index){
                      sparkline(data_spark_line$RSV[[index]], type = "line", tooltipFormat = "{{y.1}}", 
                                lineColor ="grey", fillColor = NA, 
                                lineWidth = 2, minSpotColor = F, maxSpotColor = F)
                    },name = "Trend"),
                    Denominator_RSV = colDef(name = "# tested", sortable = TRUE)),
                 
                    defaultColGroup = colGroup(headerStyle = list(
                      'font-size' = '90%',
                      'background-color' = '#69ae23',
                      'color' = '#fff')
                    ),
                    columnGroups = list(
                      colGroup(name = "", columns = c("CountryName")),
                      colGroup(name = "Consultation Rates (per 100,000)", columns = c("ARI_cases","ThresholdCode_ARICases","ILI_cases","ThresholdCode_ILICases")),
                      colGroup(name = "COVID-19", columns = c("Pos_percentage_COVID19", "COVID19","Denominator_COVID19")),
                      colGroup(name = "Influenza", columns = c("Pos_percentage_INFL", "INFL", "Denominator_INFL")),
                      colGroup(name = "RSV", columns = c("Pos_percentage_RSV", "RSV", "Denominator_RSV"))))
    
return(obj)
}

