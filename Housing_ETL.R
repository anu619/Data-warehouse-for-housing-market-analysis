library(data.table)
install.packages("hablar")
library(hablar)
library(dplyr)
install.packages("sqldf")
library(sqldf)
library(odbc)
library(DBI)

#When a new data is released the below variables should be updated with new data source URL
housing_url = "http://prod.publicdata.landregistry.gov.uk.s3-website-eu-west-1.amazonaws.com/pp-complete.csv"
tenure_url = "https://data.london.gov.uk/download/housing-tenure-borough/f125e620-cc51-4fa1-bfb4-e15d3dd1c13c/tenure-households-borough.csv"
deprivation_url = "https://opendata.camden.gov.uk/api/views/8x5x-eu22/rows.csv"

housing_1 <- fread(housing_url,select= c(1,2,3,5,6,13,14))
tenure_1 <- fread(tenure_url, select=c(1,2,3,4,5))
social_dep1 <- fread(deprivation_url, select=c(3,4,8,12,24))


#column names update
newheaders_housing <- c("sales_id",	"price_paid",	"deed_date",	"property_type"	,"new_build",	"Borough_name",	"county")
colnames(housing_1) <- newheaders_housing
newheaders_tenure <- c("Borough_code",	"Borough_name",	"Year",	"Tenure"	,"Number_of_households")
colnames(tenure_1) <- newheaders_tenure
newheaders_dep <- c("Borough_code",	"Borough_name",	"Income_score",	"Employment_score","Crime_score")
colnames(social_dep1) <- newheaders_dep

#Filtering
housing_2 <- subset(housing_1, deed_date >= '2015-01-01' & deed_date<= '2018-12-31')
tenure_2 <- subset(tenure_1, Year>= 2015 & Year<= 2018)

#Handling incorrect data
tenure_3 <- tenure_2[!(tenure_2$Tenure=="Total" | tenure_2$Borough_name=="City of London" )]
tenure_3 <- tenure_3 %>% filter(grepl("E09", tenure_3$Borough_code))
housing_3 <- housing_2[!(housing_2$property_type=="O")]


county_tmp <- c('BEDFORD','BUCKINGHAMSHIRE','CAMBRIDGESHIRE','CHESHIRE EAST', 'CHESHIRE WEST AND CHESTER','CLEVELAND','CORNWALL','CUMBRIA','DERBYSHIRE','DEVON','DORSET','DURHAM','EAST SUSSEX','ESSEX','GLOUCESTERSHIRE','GREATER LONDON','GREATER MANCHESTER','HAMPSHIRE','HERTFORDSHIRE','KENT','LANCASHIRE','LEICESTERSHIRE','LINCOLNSHIRE', 'NORTH EAST LINCOLNSHIRE', 'NORTH LINCOLNSHIRE', 'MERSEYSIDE','NORFOLK','NORTH YORKSHIRE','NORTHAMPTONSHIRE', 'NORTH NORTHAMPTONSHIRE','NORTHUMBERLAND','NOTTINGHAMSHIRE','OXFORDSHIRE','SHROPSHIRE','SOMERSET', 'NORTH SOMERSET','SOUTH YORKSHIRE','SOUTH GLOUCESTERSHIRE', 'STAFFORDSHIRE','SUFFOLK','SURREY','TYNE AND WEAR','WARWICKSHIRE','WEST BERKSHIRE','WEST MIDLANDS','WEST SUSSEX','WEST YORKSHIRE','WILTSHIRE','WORCESTERSHIRE')
housing_4 <- housing_3 %>% filter(county %in% county_tmp)
social_dep1$Borough_name <- toupper(social_dep1$Borough_name)
tenure_3$Borough_name <- toupper(tenure_3$Borough_name)

#social_dep2 <- social_dep1 %>% filter(grepl("E09", social_dep1$Borough_code) & !social_dep1$Borough_code=="E09000001" )

#Handling 'out of range' data issues
Q1 <- quantile(housing_4$price_paid, .25)
Q3 <- quantile(housing_4$price_paid, .75)
IQR <- IQR(housing_4$price_paid)
housing_5<- subset(housing_4, price_paid >(Q1 - 1.5*IQR) & price_paid< (Q3 + 1.5*IQR))

#Check for Missing values
colSums(is.na(housing_5))
colSums(is.na(tenure_3))
colSums(is.na(social_dep1))
View(tenure_3 %>% filter(is.na(Number_of_households))) # To view the entries with NA

#Data Aggregation
social_dep2 <- social_dep1 %>% group_by(Borough_code, Borough_name) %>% summarize(avg_income_score = mean(Income_score), avg_emp_score = mean(Employment_score),avg_crime_score = mean(Crime_score))
housing_6 <- housing_5 %>% mutate(Year=year(deed_date),Month=month(deed_date)) %>% select(county, Borough_name, sales_id, Year, Month, price_paid, property_type, new_build) %>% group_by(county,Borough_name, Year, Month, property_type, new_build) %>% summarise(Average_price=mean(price_paid), Sales_count=n())


#update datatypes
housing_7 <- housing_6 %>% convert(num(Average_price, Sales_count, Year, Month), fct(new_build, Borough_name,county,property_type))
tenure_3 <- tenure_3 %>% convert(int(Year, Number_of_households), fct(Tenure,Borough_code,Borough_name))
social_dep3 <- social_dep2 %>% convert(fct(Borough_code, Borough_name), num(avg_income_score, avg_emp_score, avg_crime_score))


#Assigning surrogate keys
housing_8 <- transform(housing_7, County_key = as.numeric(housing_7$county), Property_type_key = as.numeric(housing_7$property_type), New_built_key = as.numeric(housing_7$new_build))
tenure_4 <- transform(tenure_3, Tenure_key = as.numeric(tenure_3$Tenure))

#Data Merging
data_England <- merge(x=housing_8,y=social_dep3,by="Borough_name")
data_London <-  merge(x=data_England, y=tenure_4, by= c("Borough_code","Year","Borough_name"))


#Create Dimension tables
DimLocation <- sqldf('SELECT data_England.County_key, data_England.county, data_England.Borough_code, data_England.Borough_name FROM data_England GROUP BY data_England.County_key, data_England.county, data_England.Borough_code, data_England.Borough_name ORDER BY data_England.County_key, data_England.Borough_code')
DimLocation2 <- sqldf('SELECT DimLocation.County_key, DimLocation.county FROM DimLocation GROUP BY DimLocation.County_key, DimLocation.county ORDER BY DimLocation.County_key')
DimNewbuild <- sqldf('SELECT data_England.New_built_key, data_England.new_build FROM data_England GROUP BY data_England.New_built_key, data_England.new_build ORDER BY data_England.New_built_key')
DimProperty <- sqldf('SELECT data_England.Property_type_key, data_England.property_type FROM data_England GROUP BY data_England.Property_type_key, data_England.property_type')
DimTenure <- sqldf('SELECT data_London.Tenure_key, data_London.Tenure FROM data_London GROUP BY data_London.Tenure_key, data_London.Tenure ORDER BY data_London.Tenure_key')
TempDimTime <- sqldf('SELECT ([Month]*10000 + [Year]) AS TimeID, data_England.Year, data_England.Month FROM data_England GROUP BY ([Month]*10000 + [Year]), data_England.Year, data_England.Month ORDER BY data_England.Year DESC , data_England.Month')
DimTime <- sqldf('SELECT * FROM TempDimTime')

#Create Fact tables
salesfact <- sqldf('SELECT data_England.County_key, data_England.Borough_code, TempDimTime.TimeID, data_England.New_built_key, data_England.Property_type_key, Sum(data_England.Sales_count) AS Sales_count, Avg(data_England.Average_price) AS Average_price FROM data_England INNER JOIN TempDimTime ON (data_England.Year = TempDimTime.Year) AND (data_England.Month = TempDimTime.Month) GROUP BY data_England.County_key, data_England.Borough_code, TempDimTime.TimeID, data_England.New_built_key, data_England.Property_type_key')
deprivation_fact <- sqldf('SELECT data_England.County_key, data_England.Borough_code, Avg(data_England.avg_crime_score) AS avg_crime_score, Avg(data_England.avg_emp_score) AS avg_emp_score, Avg(data_England.avg_income_score) AS avg_income_score FROM data_England GROUP BY data_England.County_key, data_England.Borough_code')
tenurefact <- sqldf('SELECT data_London.County_key, data_London.Borough_code, TempDimTime.TimeID, data_London.Tenure_key, data_London.Number_of_households FROM data_London INNER JOIN TempDimTime ON (data_London.Year = TempDimTime.Year) AND (data_London.Month = TempDimTime.Month) GROUP BY data_London.County_key, data_London.Borough_code, TempDimTime.TimeID, data_London.Tenure_key, data_London.Number_of_households')


#Establish a connection with Hive
con <- dbConnect(odbc::odbc(), .connection_string = "Driver={Cloudera ODBC Driver for Apache Hive};", 
                 timeout = 10,uid="hive", Host = "sandbox-hdp.hortonworks.com",Port =10000)

#Creating Empty tables in Hive
dbGetQuery(con, "CREATE DATABASE IF NOT EXISTS HousingDB")
dbGetQuery(con,"use HousingDB")
dbGetQuery(con,"CREATE TABLE DimLocation(county_key INT, county STRING, Borough_code STRING, Borough_name STRING)")
dbGetQuery(con,"CREATE TABLE DimLocation2(county_key INT, county STRING)")
dbGetQuery(con,"CREATE TABLE DimNewbuild(New_built_key INT, new_build STRING)")
dbGetQuery(con,"CREATE TABLE DimProperty(Property_type_Key INT, property_type STRING)")
dbGetQuery(con,"CREATE TABLE DimTenure(Tenure_key INT, Tenure STRING)")
dbGetQuery(con,"CREATE TABLE TempDimTime(TimeID INT, Year INT, Month INT)")
dbGetQuery(con,"CREATE TABLE DimTime(TimeID INT, Year INT, Month INT)")
#Loading the data in R data frames to hive tables
DBI::dbWriteTable(conn = con,"DimLocation", DimLocation,append = TRUE)
DBI::dbWriteTable(conn = con,"DimLocation2", DimLocation2,append = TRUE)
DBI::dbWriteTable(conn = con,"DimNewbuild", DimNewbuild, append = TRUE)
DBI::dbWriteTable(conn = con,"DimProperty", DimProperty, append = TRUE)
DBI::dbWriteTable(conn = con,"DimTenure", DimTenure, append = TRUE)
DBI::dbWriteTable(conn = con,"TempDimTime", TempDimTime, append = TRUE)
DBI::dbWriteTable(conn = con,"DimTime", DimTime, append = TRUE)
DBI::dbWriteTable(conn = con,"salesfact", salesfact, append = TRUE)
DBI::dbWriteTable(conn = con,"deprivation_fact", deprivation_fact, append = TRUE)
DBI::dbWriteTable(conn = con,"tenurefact", tenurefact, append = TRUE)

#Schema creation
dbGetQuery(con,'CREATE TABLE Star1 as (SELECT salesfact.County_key, salesfact.Borough_code, salesfact.TimeID, salesfact.New_built_key, salesfact.Property_type_key, salesfact.Sales_count, salesfact.Average_price, deprivation_fact.avg_crime_score, deprivation_fact.avg_emp_score, deprivation_fact.avg_income_score, DimLocation.county, DimLocation.Borough_name, DimTime.Year, DimTime.Month, DimProperty.property_type, DimNewbuild.new_build FROM DimNewbuild INNER JOIN (DimProperty INNER JOIN (DimTime INNER JOIN (DimLocation2 INNER JOIN ((DimLocation INNER JOIN deprivation_fact ON (DimLocation.Borough_code = deprivation_fact.Borough_code) AND (DimLocation.Borough_code = deprivation_fact.Borough_code)) INNER JOIN salesfact ON DimLocation.Borough_code = salesfact.Borough_code) ON (DimLocation2.County_key = salesfact.County_key) AND (DimLocation2.County_key = salesfact.County_key) AND (DimLocation2.County_key = DimLocation.County_key) AND (DimLocation2.County_key = DimLocation.County_key) AND (DimLocation2.County_key = DimLocation.County_key)) ON (DimTime.TimeID = salesfact.TimeID) AND (DimTime.TimeID = salesfact.TimeID)) ON (DimProperty.Property_type_key = salesfact.Property_type_key) AND (DimProperty.Property_type_key = salesfact.Property_type_key)) ON (DimNewbuild.New_built_key = salesfact.New_built_key) AND (DimNewbuild.New_built_key = salesfact.New_built_key) GROUP BY salesfact.County_key, salesfact.Borough_code, salesfact.TimeID, salesfact.New_built_key, salesfact.Property_type_key, salesfact.Sales_count, salesfact.Average_price, deprivation_fact.avg_crime_score, deprivation_fact.avg_emp_score, deprivation_fact.avg_income_score, DimLocation.county, DimLocation.Borough_name, DimTime.Year, DimTime.Month, DimProperty.property_type, DimNewbuild.new_build'))
dbGetQuery(con, 'CREATE TABLE Star2 as (SELECT salesfact.County_key, salesfact.Borough_code, salesfact.TimeID, salesfact.New_built_key, salesfact.Property_type_key, salesfact.Sales_count, salesfact.Average_price, deprivation_fact.avg_crime_score, deprivation_fact.avg_emp_score, deprivation_fact.avg_income_score, tenurefact.Tenure_key, tenurefact.Number_of_households, DimLocation.county, DimLocation.Borough_name, DimNewbuild.new_build, DimProperty.property_type, DimTenure.Tenure, DimTime.Year, DimTime.Month FROM DimTime INNER JOIN (DimTenure INNER JOIN (DimProperty INNER JOIN (DimNewbuild INNER JOIN (DimLocation2 INNER JOIN (((DimLocation INNER JOIN deprivation_fact ON (DimLocation.Borough_code = deprivation_fact.Borough_code) AND (DimLocation.Borough_code = deprivation_fact.Borough_code)) INNER JOIN salesfact ON DimLocation.Borough_code = salesfact.Borough_code) INNER JOIN tenurefact ON (DimLocation.Borough_code = tenurefact.Borough_code) AND (DimLocation.Borough_code = tenurefact.Borough_code)) ON (DimLocation2.County_key = salesfact.County_key) AND (DimLocation2.County_key = salesfact.County_key) AND (DimLocation2.County_key = DimLocation.County_key) AND (DimLocation2.County_key = DimLocation.County_key) AND (DimLocation2.County_key = DimLocation.County_key)) ON (DimNewbuild.New_built_key = salesfact.New_built_key) AND (DimNewbuild.New_built_key = salesfact.New_built_key)) ON (DimProperty.Property_type_key = salesfact.Property_type_key) AND (DimProperty.Property_type_key = salesfact.Property_type_key)) ON (DimTenure.Tenure_key = tenurefact.Tenure_key) AND (DimTenure.Tenure_key = tenurefact.Tenure_key) AND (DimTenure.Tenure_key = tenurefact.Tenure_key)) ON (DimTime.TimeID = tenurefact.TimeID) AND (DimTime.TimeID = tenurefact.TimeID) AND (DimTime.TimeID = salesfact.TimeID) AND (DimTime.TimeID = salesfact.TimeID) GROUP BY salesfact.County_key, salesfact.Borough_code, salesfact.TimeID, salesfact.New_built_key, salesfact.Property_type_key, salesfact.Sales_count, salesfact.Average_price, deprivation_fact.avg_crime_score, deprivation_fact.avg_emp_score, deprivation_fact.avg_income_score, tenurefact.Tenure_key, tenurefact.Number_of_households, DimLocation.county, DimLocation.Borough_name, DimNewbuild.new_build, DimProperty.property_type, DimTenure.Tenure, DimTime.Year, DimTime.Month'))