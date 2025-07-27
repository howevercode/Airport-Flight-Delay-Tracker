-- Set context
USE ROLE TRAINING_ROLE;

CREATE WAREHOUSE IF NOT EXISTS LEARNER_WH INITIALLY_SUSPENDED=TRUE;
USE WAREHOUSE LEARNER_WH;

CREATE DATABASE IF NOT EXISTS LEARNER_DB;

CREATE SCHEMA IF NOT EXISTS LEARNER_DB.LEARNER_SCHEMA;
USE SCHEMA LEARNER_DB.LEARNER_SCHEMA;


-- Create an internal named stage

CREATE OR REPLACE STAGE flight_stage;


-- Create a file format

CREATE OR REPLACE FILE FORMAT flight_file_format TYPE = CSV
COMPRESSION = NONE
FIELD_DELIMITER = '|'
FILE_EXTENSION = 'tbl' 
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


-- Create the source files

COPY INTO @flight_stage
FROM (
SELECT fl_date,
       op_carrier_fl_num,
       origin,
       dest,
       TIME_FROM_PARTS(SUBSTR(crs_dep_time, 1, 2), SUBSTR(crs_dep_time, 3, 4), 0) AS crs_departure_time,
       TIME_FROM_PARTS(SUBSTR(dep_time, 1, 2), SUBSTR(dep_time, 3, 4), 0) AS departure_time,
       TIME_FROM_PARTS(SUBSTR(crs_arr_time, 1, 2), SUBSTR(crs_arr_time, 3, 4), 0) AS crs_arrival_time,
       TIME_FROM_PARTS(SUBSTR(arr_time, 1, 2), SUBSTR(arr_time, 3, 4), 0) AS arrival_time,
       IFF(crs_arrival_time <= crs_departure_time, DATEADD(day, 1, fl_date), fl_date) crs_arrival_date,
       IFF(arrival_time <= departure_time, DATEADD(day, 1, fl_date), fl_date) arrival_date,
FROM   SNOWBEARAIR_DB.RAW.ONTIME_REPORTING
WHERE year = 2015
AND length(arr_time)<>0
AND length(dep_time)<>0
AND crs_dep_time < crs_arr_time
AND dep_time < arr_time
)
FILE_FORMAT = (FORMAT_NAME = flight_file_format)
OVERWRITE=TRUE;


-- Verify the stage has files
list @flight_stage;

--         crs_departure_time stands for Computerized Reservations System. So
--         the column crs_departure_time is the scheduled departure time, and
--         crs_arrival_time is the scheduled arrival time. The columns
--         departure_time and arrival_time contain the actual departure and arrival times.
--         crs_arrival_date contains the scheduled arrival date. 
--         The arrival_date column contains the actual arrival date.
