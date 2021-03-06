Google Big Query Exercise
Link: https://console.cloud.google.com/bigquery?project=ninth-history-289717&folder=&organizationId=&j=bq:US:bquxjob_438d8d37_1749816d228&page=queryresults


1.	Find the average user age 

SELECT avg(A.age) 
FROM (SELECT 2020-member_birth_year AS age
      FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` 
      WHERE member_birth_year IS NOT NULL) A

#Age 38.99

2.	Find the average time (in minutes) to get from the San Antonio Caltrain Station to the San Antonio Shopping Center and vice versa

SELECT avg(A.MIN)/60 AS DURATION_MIN
FROM (SELECT duration_sec AS MIN
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  WHERE start_station_name LIKE '%San Antonio Caltrain Station%'
  AND end_station_name LIKE '%San Antonio Shopping Center%') A

# 6.2 min 

SELECT avg(A.MIN)/60 AS DURATION_MIN
FROM (SELECT duration_sec AS MIN
  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
  WHERE end_station_name LIKE '%San Antonio Caltrain Station%'
  AND start_station_name LIKE '%San Antonio Shopping Center%') A

# 7.2 min


3.	Find the total ride time (in hours) of the 5 most heavily ridden bikes (by time)

SELECT bike_number AS BIKE, round(duration_sec/3600,1) AS TOTAL_RIDE_TIME
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
ORDER BY duration_sec DESC
LIMIT 5 

BIKE 535: 4797.3 
BIKE 464: 593.6
BIKE 680: 514.6
BIKE 262: 314.9
BIKE 247: 200.6

4.	Find the total number of rides by gender and birth year

SELECT member_gender AS GENDER, member_birth_year AS BIRTH_YEAR, COUNT(trip_id) AS NUM_RIDES
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE member_gender IS NOT NULL
AND member_birth_year IS NOT NULL
GROUP BY member_gender, member_birth_year

5.	For each pair A, B of stations, find the number of trips from Station A to Station B per bike (treat A -> B as a different pair than B -> A)

SELECT bike_number AS BIKE_ID, count(trip_id) AS NUM_TRIPS
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE start_station_id != end_station_id
GROUP BY bike_number


6.	Find the most trips made by a single bike from Station A to Station B
SELECT bike_number AS BIKE_ID, count(trip_id) AS NUM_TRIPS
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE start_station_id != end_station_id
group by bike_number
GROUP BY 1

#BIKE ID 3091: 157 trips

7.	Find the day of the week with the highest average number of trips

SELECT A.TRIP_DATE, avg(NUM_TRIP) AS AVG_TRIP
FROM  (SELECT EXTRACT(DATE from start_date) AS TRIP_DATE, count(trip_id) AS NUM_TRIP
        FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
        GROUP BY TRIP_DATE
        ORDER BY NUM_TRIP
        LIMIT 10) A
GROUP BY A.TRIP_DATE
ORDER BY AVG_TRIP DESC
LIMIT 1

trip-date 2018-04-25
avg-trip 6377

8.	Find the average number of trips made by all bikes that have ever made a trip between the San Antonio Caltrain Station and the San Antonio Shopping Center

SELECT AVG(CNT)
FROM  (SELECT count(trip_id) AS CNT
      FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
      WHERE start_station_name LIKE '%San Antonio Caltrain%' 
      OR end_station_name LIKE '%San Antonio Shopping Center%'
      GROUP BY bike_number) A

#11.6

9.	Find the count of originating trips by region, and use it to find the region where most trips originate

SELECT B.region_id, A.start_station_id, A.TRIP_NUM
FROM  (SELECT start_station_id, count(trip_id) AS TRIP_NUM 
      FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
      GROUP BY start_station_id
      ORDER BY TRIP_NUM DESC
      LIMIT 1) A

INNER JOIN 

(SELECT station_id, region_id
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info`) B

ON A.start_station_id = B.station_id

#region_id 3
#start_station_id 70
#Trip_num 80,370



10.	Find the total count of trips by station originating in Berkeley

SELECT region_id, name
FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_regions`
WHERE NAME LIKE '%Berkeley%'

#region_id 14, name Berkeley

SELECT A.station_id, count(B.trip_id) AS TRIP_NUM
FROM  (SELECT station_id, region_id 
        FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info`
        where region_id = 14) A

INNER JOIN

(SELECT start_station_id, trip_id
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`) B

ON A.station_id = B.start_station_id
GROUP BY A.station_id
