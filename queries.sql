SELECT 
  COUNT(DISTINCT *)
FROM
  `bigquery-public-data.london_bicycles.cycle_hire`
GROUP BY
  start_date, start

-- Checking outliers for station IDs
SELECT
  COUNT(*) AS too_small
FROM
  `bigquery-public-data.london_bicycles.cycle_hire`
WHERE
  start_station_id < 1
  OR end_station_id < 1

/*
too_small
0

[It seems like the `start_station_logical_terminal` and `end_station_logical_terminal` columns replace the station IDs? (https://github.com/ropensci/bikedata/issues/21).
Not really sure what the priority ID of `end_station_priority_id` refers to?]
All IDs are larger than 1.
*/
--

SELECT
  COUNT(*) AS too_large
FROM
  `bigquery-public-data.london_bicycles.cycle_hire`
WHERE
  start_station_id > 852
  OR end_station_id > 852

/*
too_large
2386723

It can be seen that a large number of trips have an ID larger than the 852 observed in the station's table.
*/

-- Let's find the number of distinct stations Names
SELECT
  COUNT(DISTINCT start_station_name) AS nunique_start_station_name,
  COUNT(DISTINCT end_station_name) AS nunique_end_station_name,
FROM
  `bigquery-public-data.london_bicycles.cycle_hire`

/*
nunique_start_station_name	nunique_end_station_name
954	957

We can see there are ~150 more stations than those observed in the station's table.
*/

-- Let's check the start and end station names in both tables
SELECT
  name
FROM
  (SELECT
    DISTINCT start_station_name AS name  -- or `end_station_name`
  FROM
    `bigquery-public-data.london_bicycles.cycle_hire`) t1
JOIN
  (SELECT
    DISTINCT name
  FROM
    `bigquery-public-data.london_bicycles.cycle_stations`) t2
USING
  (name)

/*
We can see the station names belonging to both table.
*/

-- Extracting all hires with identified station names
WITH stations AS (
  SELECT
    name
  FROM
    (SELECT
      DISTINCT start_station_name AS name
    FROM
      `bigquery-public-data.london_bicycles.cycle_hire`) t1
  JOIN
    (SELECT
      DISTINCT name
    FROM
      `bigquery-public-data.london_bicycles.cycle_stations`) t2
  USING
    (name)
)

SELECT
  bike_id, start_date, start_station_name, end_date, end_station_name, duration
FROM 
  `bigquery-public-data.london_bicycles.cycle_hire`
WHERE
  start_station_name IN (SELECT name FROM stations)
  AND end_station_name IN (SELECT name FROM stations)
ORDER BY
  start_date

/*
We'll save the result of this query into a new table `hires`
*/


-- Removing null start and end dates, and start dates greater than the end dates
SELECT 
  *
FROM
  `modern-crane-397209.santander_bikes.hires`
WHERE
  start_date IS NOT NULL
  AND end_date IS NOT NULL
  AND start_date < end_date

/*
There are ~75,000,000 results in this query
*/

-- Checking for durations not matching with their respective start and end dates
WITH clean_hires AS (
  SELECT 
    *
  FROM
    `modern-crane-397209.santander_bikes.hires`
  WHERE
    start_date IS NOT NULL
    AND end_date IS NOT NULL
    AND start_date < end_date
)

SELECT
  start_date, end_date, duration, TIMESTAMP_DIFF(end_date, start_date, SECOND) AS calculated_duration
FROM
  clean_hires
WHERE
  duration IS NOT NULL
  AND duration != TIMESTAMP_DIFF(end_date, start_date, SECOND)

/*
This yields ~3,000,000 results. Since this represents only (3 / 75 =) 4 % of the table, in the interest of time, we will discard these rows of possible dirty data.
*/

-- Final cleaning query
WITH clean_hires AS (
  SELECT 
    *
  FROM
    `modern-crane-397209.santander_bikes.hires`
  WHERE
    start_date IS NOT NULL
    AND end_date IS NOT NULL
    AND start_date < end_date
    AND duration IS NOT NULL
    AND duration = TIMESTAMP_DIFF(end_date, start_date, SECOND)
)


-- Other queries
SELECT
  start_station_name, end_station_name, COUNT(*) AS num_trips, ROUND(AVG(duration / 60), 2) as duration_min
FROM
  clean_hires
GROUP BY
  start_station_name, end_station_name
ORDER BY
  num_trips DESC


-- Grouping the trips into one-hour intervals across the entire table
SELECT
  TIMESTAMP_TRUNC(start_date, HOUR) AS start_hour,
  TIMESTAMP_TRUNC(end_date, HOUR) AS end_hour,
  start_station_name,
  end_station_name, 
  COUNT(*) AS num_trips
FROM
  clean_hires
GROUP BY
  start_station_name, end_station_name, start_hour, end_hour
ORDER BY
  start_station_name, end_station_name, start_hour

SELECT 
  dt as time_date,
  start_station_name,
  end_station_name
FROM 
  `project-2-404313.santander_bikes.hires-num-trunc-hour`,
  UNNEST(GENERATE_TIMESTAMP_ARRAY(
    TIMESTAMP_TRUNC(start_hour, HOUR),
    TIMESTAMP_TRUNC(end_hour, HOUR),
    INTERVAL 1 HOUR
  )) as dt
ORDER BY
  start_station_name, end_station_name, time_date

WITH hires_num_flatten_interval_hour AS
  (SELECT 
    dt as time_date,
    start_station_name,
    end_station_name,
    num_trips AS num_trips_1
  FROM 
    `project-2-404313.santander_bikes.hires-num-trunc-hour`,
    UNNEST(GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(start_hour, HOUR),
      TIMESTAMP_TRUNC(end_hour, HOUR),
      INTERVAL 1 HOUR
    )) as dt
  ORDER BY
    start_station_name, end_station_name, time_date)

SELECT
  time_date,
  start_station_name,
  end_station_name,
  SUM(num_trips_1) AS num_trips
FROM
  hires_num_flatten_interval_hour
GROUP BY
  time_date, start_station_name, end_station_name
ORDER BY
  time_date


-- Adding borough information to the data
SELECT
  time_date,
  start_station_name,
  ward AS start_station_ward,
  borough AS start_station_borough,
  end_station_name,
  end_station_ward,
  end_station_borough,
  num_trips
FROM
  (SELECT
    time_date,
    start_station_name,
    end_station_name,
    ward AS end_station_ward,
    borough AS end_station_borough,
    num_trips
  FROM
    `project-3-404315.santander_bikes.hires-num-per-hour` t1
  JOIN
    `project-3-404315.santander_bikes.station-w-ward` t2
  ON
    t1.end_station_name = t2.name) t1
JOIN
  `project-3-404315.santander_bikes.station-w-ward` t2
ON
  t1.start_station_name = t2.name


-- Getting the total number of trips between London boroughs of the stations
WITH hires_num_station AS (SELECT 
  start_station_name,
  end_station_name, 
  COUNT(*) AS num_trips_1
FROM
  `modern-crane-397209.santander_bikes.hires`
WHERE
  start_date IS NOT NULL
  AND end_date IS NOT NULL
  AND start_date < end_date
  AND duration IS NOT NULL
  AND duration = TIMESTAMP_DIFF(end_date, start_date, SECOND)
GROUP BY
  start_station_name, end_station_name
ORDER BY
  start_station_name, end_station_name
)

SELECT
  borough AS start_station_borough,
  end_station_borough,
  SUM(num_trips_1) AS num_trips
FROM
  (SELECT
    start_station_name,
    borough AS end_station_borough,
    num_trips_1
  FROM
    hires_num_station t1
  JOIN
    `project-3-404315.santander_bikes.station-w-ward` t2
  ON
    t1.end_station_name = t2.name) t1
JOIN
  `project-3-404315.santander_bikes.station-w-ward` t2
ON
  t1.start_station_name = t2.name
GROUP BY
  start_station_borough, end_station_borough
ORDER BY
  start_station_borough, end_station_borough


-- Computing the average daily bike flux (see 'avg_chg_bicycles_') for each station
WITH flux_station_day AS (SELECT
    *,
    num_ends - num_starts AS chg_bicycles
  FROM
    (SELECT
      start_station_name AS station_name,
      TIMESTAMP_TRUNC(start_date, DAY) as day_,
      COUNT(*) AS num_starts
    FROM
      `project-5-405413.santander_bikes.hires-clean`
    GROUP BY
      station_name, day_
    ORDER BY
      station_name, day_)
  FULL OUTER JOIN
    (SELECT
      end_station_name AS station_name,
      TIMESTAMP_TRUNC(end_date, DAY) as day_,
      COUNT(*) AS num_ends
    FROM
      `project-5-405413.santander_bikes.hires-clean`
    GROUP BY
      station_name, day_
    ORDER BY
      station_name, day_)
  USING
    (station_name, day_)
ORDER BY
  station_name, day_
)

-- SELECT
--   'All stations' AS station_name,
--   EXTRACT(YEAR from day_) AS year_,
--   AVG(chg_bicycles) AS avg_chg_bicycles,
-- FROM
--   flux_station_day
-- GROUP BY
--   year_
-- HAVING
--   year_ != 2023
-- ORDER BY
--   year_

SELECT
  station_name,
  year_,
  CAST(ROUND(avg_chg_bicycles, 0) AS INT) AS avg_chg_bicycles_  -- Converting average to an integer
FROM
  (SELECT
    station_name,
    EXTRACT(YEAR from day_) AS year_,
    AVG(chg_bicycles) AS avg_chg_bicycles,
  FROM
    flux_station_day
  GROUP BY
    station_name, year_
  HAVING
    year_ != 2023)  -- Ignoring trips from the year 2023
JOIN  -- Only keeping stations where the average bike flux was equal to or larger than 1 in year 2022
  (SELECT
    station_name,
    EXTRACT(YEAR from day_) AS year_1,
    AVG(chg_bicycles) AS avg_chg_bicycles_1,
  FROM
    flux_station_day
  GROUP BY
    station_name, year_1
  HAVING
    year_1 = 2022
    AND ABS(avg_chg_bicycles_1) >= 1
  ORDER BY
    station_name, year_1)
USING
  (station_name)
ORDER BY
  station_name,
  year_ 



