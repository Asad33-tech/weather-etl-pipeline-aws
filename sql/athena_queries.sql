-- Create Database
CREATE DATABASE IF NOT EXISTS weather_db;

-- Create External Table
CREATE EXTERNAL TABLE IF NOT EXISTS weather_db.weather (
    date STRING,
    temp FLOAT,
    humidity INT,
    description STRING,
    temp_category STRING
) 
STORED AS TEXTFILE
LOCATION 's3://islamabad-weather-data-project/'
TBLPROPERTIES ('has_encrypted_data'='false');

-- View all weather data
SELECT * FROM weather_db.weather ORDER BY date DESC LIMIT 10;

-- Average temperature
SELECT AVG(temp) as average_temperature FROM weather_db.weather;

-- Weather category distribution
SELECT temp_category, COUNT(*) as count 
FROM weather_db.weather 
GROUP BY temp_category;
