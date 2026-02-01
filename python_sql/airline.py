#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 25 17:28:57 2026

@author: agataszkoda
"""

import os
import sqlite3
import pandas as pd

try:
    os.remove('airline2.db')
except OSError:
    pass

#connect to the SQLite driver to manipulate the database
con = sqlite3.connect('airline2.db')

#creating tables from csv
ontime_2006 = pd.read_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/2006.csv")
ontime_2007 = pd.read_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/2007.csv")
ontime_2008 = pd.read_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/2008.csv")

#writing records stored in DataFrame into db as one table
ontime_2006.to_sql('ontime', con = con, index = False, if_exists='replace') 
ontime_2007.to_sql('ontime', con = con, index = False, if_exists='append') 
ontime_2008.to_sql('ontime', con = con, index = False, if_exists='append') 

#creating the rest of the tables from csv
airports = pd.read_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/airports.csv")
carriers = pd.read_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/carriers.csv")
planes = pd.read_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/dataverse_files/plane-data.csv")

#writing records stored in DataFrame into db as tables
airports.to_sql('airports', con = con, index = False) 
carriers.to_sql('carriers', con = con, index = False) 
planes.to_sql('planes', con = con, index = False) 

# Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?
q1 = '''
SELECT 
model AS model, 
AVG(ontime.DepDelay) AS avg_delay 
FROM planes JOIN ontime USING(tailnum) 
WHERE ontime.Cancelled = 0 AND ontime.Diverted = 0 AND ontime.DepDelay > 0 
GROUP BY model 
ORDER BY avg_delay
'''
df1 = pd.read_sql_query(q1, con)
df1.to_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/st2195_assignment_3/python_sql/q1.csv", index=False)

# Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
q2 = '''
SELECT airports.city AS city, COUNT(*) AS total
FROM airports JOIN ontime ON ontime.dest = airports.iata
WHERE ontime.Cancelled = 0
GROUP BY airports.city
ORDER BY total DESC
'''
df2 = pd.read_sql_query(q2, con)
df2.to_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/st2195_assignment_3/python_sql/q2.csv", index=False)

# Which of the following companies has the highest number of cancelled flights?
q3 = '''
SELECT carriers.Description AS carrier, COUNT(*) AS total
FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
WHERE ontime.Cancelled = 1
AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
GROUP BY carriers.Description
ORDER BY total DESC
'''
df3 = pd.read_sql_query(q3, con)
df3.to_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/st2195_assignment_3/python_sql/q3.csv", index=False)

# Which of the following companies has the highest number of cancelled flights, relative to their number of total flights?
q4 = '''
SELECT
    q1.carrier AS carrier, (CAST(q1.numerator AS FLOAT)/ CAST(q2.denominator AS FLOAT)) AS ratio
FROM
(
    SELECT carriers.Description AS carrier, COUNT(*) AS numerator
    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
    WHERE ontime.Cancelled = 1 AND carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
    GROUP BY carriers.Description
) AS q1 JOIN
(
    SELECT carriers.Description AS carrier, COUNT(*) AS denominator
    FROM carriers JOIN ontime ON ontime.UniqueCarrier = carriers.Code
    WHERE carriers.Description IN ('United Air Lines Inc.', 'American Airlines Inc.', 'Pinnacle Airlines Inc.', 'Delta Air Lines Inc.')
    GROUP BY carriers.Description
) AS q2 USING(carrier)
ORDER BY ratio DESC
'''
df4 = pd.read_sql_query(q4, con)
df4.to_csv("/Users/agataszkoda/Documents/Studia Data Science and Business Analytics LSE/Programming for Data Science/st2195_assignment_3/python_sql/q4.csv", index=False)

con.close()