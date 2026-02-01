library(DBI)
library(RSQLite)
library(dplyr)

if (file.exists("airline2.db")) {
  file.remove("airline2.db")
}

# creating airline2.db
con <- dbConnect(SQLite(), "airline2.db")

# loading 2006 data
ontime_2006 <- read.csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/2006.csv", header = TRUE)
dbWriteTable(con, "ontime", ontime_2006, overwrite = TRUE)

# adding 2007 data
ontime_2007 <- read.csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/2007.csv", header = TRUE)
dbWriteTable(con, "ontime", ontime_2007, append = TRUE)

# adding 2008 data
ontime_2008 <- read.csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/2008.csv", header = TRUE)
dbWriteTable(con, "ontime", ontime_2008, append = TRUE)

# loading airports
airports <- read.csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/airports.csv", header = TRUE)
dbWriteTable(con, "airports", airports, overwrite = TRUE)

# loading carriers
carriers <- read.csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/carriers.csv", header = TRUE)
dbWriteTable(con, "carriers", carriers, overwrite = TRUE)

# loading plane-data
planes <- read.csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/plane-data.csv", header = TRUE)
dbWriteTable(con, "planes", planes, overwrite = TRUE)


ontime_db <- tbl(con, "ontime")
airports_db <- tbl(con, "airports")
carriers_db <- tbl(con, "carriers")
planes_db <- tbl(con, "planes")

# Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?
q1 <- inner_join (ontime_db, planes_db, by = c("TailNum"="tailnum")) %>%
  filter (Cancelled == 0, Diverted == 0, DepDelay > 0) %>%
  group_by (model) %>%
  summarize(avg_dep_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_dep_delay)
q1
write.csv(q1, "q1_dplyr.csv", row.names = FALSE)

# Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
q2 <- inner_join (airports_db, ontime_db, by = c("iata"="Dest")) %>%
  filter (Cancelled == 0) %>%
  count(city, sort = TRUE)
q2
write.csv(q2, "q2_dplyr.csv", row.names = FALSE)

# Which of the following companies has the highest number of cancelled flights?
q3 <- inner_join (carriers_db, ontime_db, by = c("Code"="UniqueCarrier")) %>%
  filter (Cancelled == 1, Description %in% c("United Air Lines Inc.", "American Airlines Inc.", "Pinnacle Airlines Inc.", "Delta Air Lines Inc.")) %>%
  count(Description, sort = TRUE, name = 'cancelled_flights')
q3
write.csv(q3, "q3_dplyr.csv", row.names = FALSE)

# Which of the following companies has the highest number of cancelled flights, relative to their number of total flights?

# calculating total flights
qtotal <- inner_join (carriers_db, ontime_db, by = c("Code"="UniqueCarrier")) %>%
  filter(Description %in% c("United Air Lines Inc.", "American Airlines Inc.", "Pinnacle Airlines Inc.", "Delta Air Lines Inc.")) %>%
  count(Description, sort = TRUE, name = 'total_flights')
qtotal

# calculating cancelled flights relative to total flights
q4 <- inner_join(q3, qtotal, by = "Description") %>%
  mutate(cancelled_ratio = cancelled_flights*1.0/total_flights) %>%
  arrange(desc(cancelled_ratio))
q4_df <- q4 %>%
  collect() %>% 
  arrange(desc(cancelled_ratio))
q4_df
write.csv(q4_df, "q4_dplyr.csv", row.names = FALSE)

dbDisconnect(con)

