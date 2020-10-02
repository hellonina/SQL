GOOGLE SQL 


#1.Use a window to find the id of the first station accessed each day (this can also be done with grouping)
SELECT A.date_started, A.first_station_id
FROM (SELECT EXTRACT(DATE FROM start_date) AS date_started, 
			FIRST_VALUE(start_station_id) OVER (PARTITION BY EXTRACT(DATE FROM start_date) ORDER BY start_date ASC) AS first_station_id
	  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`) AS A
GROUP BY A.date_started, A.first_station_id


/*<output>
date station_id 
2013-08-29 64
2013-08-30 47
2013-08-31 76

2.Use LAST_VALUE to find the lASt date each bike wAS ridden.*/

SELECT bike_number,  EXTRACT(DATE FROM end_date_per_bike) AS end_date_per_bike
FROM (SELECT bike_number, 
	         LAST_VALUE(end_date) OVER (PARTITION BY EXTRACT (DATE FROM end_date) ORDER BY end_date ASC) AS end_date_per_bike
	  FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
GROUP BY bike_number, EXTRACT(DATE FROM end_date_per_bike) AS end_date_per_bike

/*output
bike_number | end_date_per_bike
288 		2013-08-29


3.Find the average number of trips per bike per day OVER a 3 day interval starting FROM the current day*/

SELECT bike_number, 
	   date_started, 
	   AVG(trip_cnt) OVER (PARTITION BY bike_number ORDER BY date_started ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS AVG_trip_per_bike_per_day
FROM (SELECT bike_number, 
	  EXTRACT(DATE FROM start_date) AS date_started, 
      COUNT(trip_id) AS trip_cnt 
      FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
GROUP BY bike_number, EXTRACT(DATE FROM start_date))


/*<output>
bike_number| date_started | AVG_trip_per_bike_per_day
9			2013-08-29		1.66
9			2013-08-30		2.33


4.Determine WHERE each station RANKs per day in number of trips originating at that station. 
Include both Olympic style and non-Olympic style ordering. 
Find an example of a station whose RANK differs between the two orderings on some date.*/

Olympic style

SELECT start_station_id, date_started, trip_cnt, RANK() OVER (ORDER BY trip_cnt DESC) AS station_RANK
FROM (SELECT EXTRACT(DATE FROM start_date) AS date_started, start_station_id, COUNT(trip_id) AS trip_cnt 
FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` GROUP BY start_station_id, EXTRACT(DATE FROM start_date))

<output>
station_id | date_started | trip_cnt | RANK_per_day 
30 			2018-04-25		167			1
30			2018-04-09		159			2
30			2018-02-05		150			3

Non-olympic
SELECT start_station_id, date_started, trip_cnt, DENSE_RANK() OVER (ORDER BY trip_cnt DESC) AS station_RANK
FROM (SELECT EXTRACT(DATE FROM start_date) AS date_started, start_station_id, COUNT(trip_id) AS trip_cnt 
FROM  `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips` GROUP BY start_station_id, EXTRACT(DATE FROM start_date))

<output>
station_id | date_started | trip_cnt | RANK_per_day 
30 			2018-04-25		167			1
30			2018-04-09		159			2
30			2018-02-05		150			3




#5.How many trips occurred WHERE the previous recorded end station for a bike wAS different FROM the current start station? 
#Do not include a bike’s first trip in the COUNT.

SELECT bike_number, COUNT(trip_id) OVER (PARTITION BY bike_number ORDER BY start_date ASC rows between 1 preceding and current row)
FROM (SELECT bike_number, trip_id, start_station_id, end_station_id, start_date
FROM `bigquery-public-data.san_francisco_bikeshare.bikeshare_trips`
WHERE start_station_id != end_station_id)




/*1. Temporary Tables
- Ephemeral: live in memory, disappear at the end of user's session
- Repeated interaction with same datASet
- Help reducing the complexity of queries*/
CREATE TEMPORARY TABLE temp_table AS
(SELECT ..
 FROM ..);
 
/*2. Window Functions
- Perform calculations across a set of rows that are somehow related to the current row
- Similar to aggregate functions but gives finer control
- 1) Aggregate : SUM, MIN, COUNT, AVG
- 2) RANKing: ROW_NUMBER, RANK, DENSE_RANK, N_TILE
- 3) Distribution/Analytics : CUME_DIST, FIRST_VALUE, LAG, PERCENTILE_CONT, PERCENTILE_DISC, PERCENTILE_RANK, LEAD*/

ex. SUM(revenue) 
	OVER (						 -- window function
			PARTITION BY siteID  -- partition clause
            ORDER BY yearmonth 	 -- order clause
            ROWS/RANGE BETWEEN   -- frame clause, recommend using rows
				2 PRECEDING
                AND CURRENT ROW
			)
            
/*- Window function must have an 'OVER' clause
- Must have some combination of partition, ORDER BY and frame clause
- For each row, only consider results in that window
- Want to calculate revenue column > but want to group those by site id
> within groups order rows by yearmonth (if they exist)

- Frame Boundaries
1) Unbounded preceding
2) <n> Preceding
3) Current Row
4) <n> Following
5) Unbounded following

- WINDOW FRAME FUNCTIONS
- FIRST_VALUE (LAST_VALUE): returns value evaluated at the row that is first (lASt) row of the window frame
: When evaluating LAST_VALUE specify frame, otherwise SQL uses ORDER BY
- NTH_VALUE
- RANK ASsigns a sequential order, olympic-medal style ordering (tie in 3rd place, then no 4th)
- DENSE_RANK : tie in 3rd place but still hAS 4th 
- LAG (LEAD) : returns value evaluated at the row that is offset rows before (after) the current row within the partition

ex. 
<sales>
product_id | product_name | revenue | date
120			Isla Teak		 625.17	2015-01-17
120			Isla Teak		312.58	2015-01-18
120			Isla Teak		907.59	2015-01-19
175			Hay Wicker		1210.12	2015-01-16
175			Hay Wicker		1976.12	2015-01-17
175			Hay Wicker		988.1	2015-01-18*/

SELECT product_id, FIRST_VALUE(revenue)
OVER (PARTITION BY product_id ORDER BY date ASC) AS first_day_rev
FROM sales;

<output>
product_id | first_day_rev
120			625.17
175			1210.12

-- You must do 'ORDER BY' to get the first value bc sql will not know what first value is. 


/*ex.
<yearly_sales>
category | revenue | year
furniture 625.17	2015
furniture 312.58	2016
furniture 907.59	2017
furniture 1210.12	2018
bedding	  1976.2	2015
bedding   988.1		2016
bedding	  297.04	2017

Compare category year revenue with previous year*/

SELECT category, year, revenue, LAG (revenue,1) -- look back 1 year
OVER (PARTITION BY category ORDER BY year ASC) AS pre_rev
FROM yearly_sales;

/*<output>
category | year | rev | prev_rev
furniture 2015  625.17 null
furniture 2016  312.58 625.17
furniture 2017  907.59 312.58
furniture 2018  1210.12 907.59
bedding   2015  1976.2  null
bedding   2016  988.1   1976.2
bedding   2017  297.04  988.1 


3. Window Function - with frame

For every row, look back and add up

ex. Find cumulative revenue for each product using sales table*/

SELECT product_id, date, SUM(revenue)
OVER (PARTITION BY product_id ORDER BY date ASC
		ROWS BETWEEN --frame clause, 'between' so you need both the start (unbounded preceding) and the end (current row)
				UNBOUNDED PRECEDING  -- row going back to the beginning of my frame
                AND CURRENT ROW) AS cumulative_rev --consider all the way to my current row
FROM sales;

/*<output>
product_id | date | cumulative_rev
120			2015-01-17 625.17
120			2015-01-18	937.75
120 		2015-01-19  1845.34
175			2015-01-17	1210.12


ex. Find rolling 3-day average revenue for each product FROM sales table*/

SELECT product_id, date, revenue, AVG(revenue) 
OVER (PARTITION BY product_id ORDER BY date ASC
	ROWS BETWEEN 
		1 PRECEDING
        AND 1 FOLLOWING) AS AVG_rev
FROM sales;

/*product_id | date | revenue | AVG_rev 
-- revenue is FROM the sales table
-- AVG_rev will be calculated within the window
-- so the OVERall OVER clause will apply only on AVG(revenue)


 product_id | date | revenue | AVG_rev
120			2015-01-17 625.17 468.88  -- (625.17+312.58)/2, the first row so there is no 1 preceding. so just AVG current and the next one, same for lASt row (gets the AVG of the one before and the current row)
120			2015-01-18 312.58 615.11
120 		2015-01-19 907.59 810.10


4. Window summary
- RANKing results within a specific window (per-group RANKing)
- Accessing data FROM another row 


ex. */

SELECT question_COUNTs.q_dt
	, AVG(question_COUNTs.q_ct) OVER (ORDER BY q_dt ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING) AS AVG_q_ct
FROM (
		SELECT EXTRACT(DATE FROM q.creation_date) AS q_dt,
			   COUNT(id) AS q_ct
		FROM 'bigquery-public-data.stackOVERflow.posts_questions' 
        GROUP BY q_dt
	) AS question_COUNTs
    
/*<output>
q_dt 		| AVG_q_ct
2008-07-31   27.5 */



