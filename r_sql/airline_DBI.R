library(DBI)
library(RSQLite)

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

# Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?
dbGetQuery(con, 
"SELECT model AS model, AVG(ontime.DepDelay) AS avg_delay
FROM planes JOIN ontime USING(tailnum)
WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0
GROUP BY model
ORDER BY avg_delay")

# Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
dbGetQuery(con, 
"SELECT airports.city AS city, COUNT(*) AS total
FROM airports JOIN ontime ON ontime.dest = airports.iata
WHERE ontime.Cancelled = 0
GROUP BY airports.city
ORDER BY total DESC")

# Which of the following companies has the highest number of cancelled flights?
dbGetQuery(con, 
"SELECT carriers.Description AS carrier, COUNT(*) AS total
FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1
AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
GROUP BY carriers.Description
ORDER BY total DESC")

dbDisconnect(con)
