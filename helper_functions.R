# Helper functions

EUnames <- c('AT', 'BE', 'BG', 'HR', 'CY', 'CZ', 'DK', 'EE', 'FI', 'FR', 'DE', 'EL', 'HU',
             'IS', 'IE', 'IT', 'LV', 'LI', 'LT', 'LU', 'MT', 'NL', 'NO', 'PL', 'PT', 'RO',
             'SK', 'SI', 'ES', 'SE')

date_rep <- function(x, year_week_W, add_W = F, day_number = "7"){
  year_week_W <- enquo(year_week_W)
  if(nrow(x)>0){
    if(add_W == T ){
      x = x %>% mutate(!!year_week_W:= str_replace(!!year_week_W, '-', '-W'))
    }
    x =   x %>% mutate(date_rep = ISOweek2date(paste(!!year_week_W, sep = '-', day_number)))
  }
  
  return(x)
}

CountryCode <- read_excel("CountryCode.xlsx")
SeasonbyWeek <-read.csv("SeasonWeek.csv")

wk2date = function(x, year_week_W, add_W = F, day_number = "7"){
  year_week_W <- enquo(year_week_W)
  
  if(nrow(x)>0){
    
    if(add_W == T ){
      x = x %>% mutate(!!year_week_W:= str_replace(!!year_week_W, '-', '-W'))
    }
    
    x =   x %>% mutate(date_rep = ISOweek2date(paste(!!year_week_W, sep = '-', day_number)))
  }
  
  return(x)
}

report_week_W = str_replace(report_week, '-', '-W')
min_date_W = str_replace(min_date, '-', '-W')
report_week_number = word(report_week, sep='-', 2) %>% as.numeric()

report_week_Sunday  = ISOweek::ISOweek2date(paste(as.character(report_week_W), "7", sep = "-"))

report_begin <- ISOdate(2009,01,01)  ## edited to start of 2020
#weeks_since_begin <- as.integer(difftime(report_date, report_begin, units="weeks")) # GF this seemed to give the wrong result and was replicated so kept next two lines instead
year_begin <- ISOdate(2009,01,01) ## edited to start of 2020
weeks_since_begin <- as.integer(difftime(report_week_Sunday, year_begin, units="weeks"))  ## edited to use report_week_Sunday

agg_weeks <- str_remove(ISOweek(seq(c(report_begin), by = "week", length.out=weeks_since_begin+1 )), 'W') # NEW
agg_weeks_numbers = word(agg_weeks, sep = '-', 2)
agg_weeks_W = str_replace(agg_weeks, '-', '-W')


agg_weeks_lookup = tibble(year_week_W = agg_weeks_W, 
                          year_week = agg_weeks) %>% 
  wk2date(year_week_W, add_W = F) %>% 
  mutate(year = word(year_week, 1, sep = '-'), 
         week = word(year_week, 2, sep = '-'), 
         week_no = row_number(),
         use_ncovvacc_prev_wk = ifelse(row_number()%%2==0, F, T)) %>% left_join(.,SeasonbyWeek)

