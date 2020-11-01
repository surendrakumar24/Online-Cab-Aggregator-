-- IMPORTANT: BEFORE CREATING ANY TABLE, MAKE SURE YOU RUN THIS COMMAND 

ADD JAR /opt/cloudera/parcels/CDH/lib/hive/lib/hive-hcatalog-core-1.1.0-cdh5.11.2.jar;

-- IMPORTANT: BEFORE PARTITIONING ANY TABLE, MAKE SURE YOU RUN THESE COMMANDS 
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

-- CREATE EXTERNAL TABLE 
create external table if not exists nyc_jd_raw
(
VENDORID INT,
TPEP_PICKUP_DATETIME TIMESTAMP,
TPEP_DROPOFF_DATETIME TIMESTAMP,
PASSENGER_COUNT INT,
TRIP_DISTANCE DOUBLE,
RATECODEID INT,
STORE_AND_FWD_FLAG string,	
PULOCATIONID int,
DOLOCATIONID int,
PAYMENT_TYPE int,
FARE_AMOUNT double,
EXTRA double,
MTA_TAX double,
TIP_AMOUNT double,
TOLLS_AMOUNT double,
IMPROVEMENT_SURCHARGE double,
TOTAL_AMOUNT double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/common_folder/nyc_taxi_data/'
tblproperties ("skip.header.line.count"="1");


-- The table is created for data quality check
---------------------------------------------------
--Basic Data Quality Checks

/*Q.How many records has each TPEP provider provided? Write a query that summarises the number of records of each provider.
*/
--Solution : 

select vendorid,count(*) as Records from nyc_jd_raw group by vendorid;

--Output
/*
vendorid	records
2			647183
1			527386
*/
----------------------------------------------
/*
Q.The data provided is for months November and December only. Check whether the data is consistent, and if not, identify the data quality issues. Mention all data quality issues in comments.

*/
--Solution : 
--Checking the pickup time column namely TPEP_PICKUP_DATETIME

SELECT VENDORID,
MONTH(TPEP_PICKUP_DATETIME) AS MONTH,
YEAR(TPEP_PICKUP_DATETIME) AS YR,
COUNT(1) AS RECORDS 
FROM NYC_JD_RAW 
GROUP BY VENDORID,
MONTH(TPEP_PICKUP_DATETIME),
YEAR(TPEP_PICKUP_DATETIME) 
ORDER BY VENDORID,
MONTH,YR;

/*
Inference :
The pickup times of the vendorid 1( Creative Mobile Technologies , LLC) are consistent, it contains rows only of November and December of the year 2017, whereas vendorid 2(Verifone Inc) has errorneous data from other months and years as well . 
*/

--Checking the pickup time column namely TPEP_DROPOFF_DATETIME

SELECT VENDORID,
MONTH(TPEP_DROPOFF_DATETIME) AS MONTH,
YEAR(TPEP_DROPOFF_DATETIME) AS YR,
COUNT(1) AS RECORDS 
FROM NYC_JD_RAW 
GROUP BY VENDORID,
MONTH(TPEP_DROPOFF_DATETIME),
YEAR(TPEP_DROPOFF_DATETIME) 
ORDER BY VENDORID,
MONTH,YR;

/*
Inference :
The dropoff times of the vendorid 1( Creative Mobile Technologies , LLC) are consistent apart from one record which has the dropoff in the year 2019, rest of the trips which have dropoff years as 2018 are the ones which have been pickedup at the night of last day of the year 2017, so these rows are useful.

Vendorid 2(Verifone Inc)  has few inconsistent rows which have years 2003,2008,2009 and few trips of the month of October in the year 2017.
Other than these data looks proper.
*/


------------------------------------------------
/*
Q.You might have encountered unusual or erroneous rows in the dataset. Can you conclude which vendor is doing a bad job in providing the records using different columns of the dataset? Summarise your conclusions based on every column where these errors are present. For example,  There are unusual passenger count, i.e. 0 which is unusual.
*/
--Solution:

--Checking the passenger count column
select vendorid,count(1) as records
from nyc_jd_raw
where passenger_count=0
group by vendorid
;
/*
Output

vendorid	records
2			11
1			6813
*/
--Inference : 
/*    
Passenger count cannot be zero for a trip, clearly vendor 2 (Verifone Inc) has less errorneous rows than vendor 1 (Creative Mobile Technologies , LLC)
*/


--Checking Column ratecodeid

select vendorid,count(1) as records
from nyc_jd_raw
where ratecodeid not in (1,2,3,4,5,6)
group by vendorid
;
/*
Output:

vendorid	records
1			8
2			1

Inference : 
As per the data description the ratecodeid cannot have values other than 1,2,3,4,5,6.
Hence clearly Vendor 2 (Verifone Inc) is doing a better job than Vendor 1 here.
*/

--Checking the fare_amount Column
select vendorid,count(1) as records
from nyc_jd_raw
where fare_amount <0
group by vendorid
;
/*
Output:

vendorid	records
2			558

Inference :
Fare_amount cannot be negative, Vendor 2 has negative column values
*/


--Checking the extra column

select vendorid,count(1) as records
from nyc_jd_raw
where extra <0
group by vendorid
;

/*
Output :
vendorid	records
2			285
1			1

Inference:
Extra column cannot have negative values,
Vendor 2 has more errorneous records than Vendor 1
*/

--Checkiing Mta_Tax column
select vendorid,count(1) as records
from nyc_jd_raw
where mta_tax not in (0,0.5)
group by vendorid
;
/*
Output:
 
vendorid	records
2			547
1			1

Inference:

Mta_tax can hold values 0 and 0.5,
Clearly Vendor 1 has more consistent records than Vendor 2
*/

--Checking Improvement_surcharge column,
select vendorid,count(1) as records
from nyc_jd_raw
where Improvement_surcharge not in (0,0.3)
group by vendorid
;
/*
Output:

vendorid	records
2			562

Inference: 
Improvement_surcharge can hold values 0 and 0.3,
Vendor 2 has multiple error values, Vendor 1 has none

*/

--Checking the column tip_amount
select vendorid,count(1) as records
from nyc_jd_raw
where tip_amount < 0
group by vendorid
;

/*
Output:
vendorid	records
2			4

Inference:
TIP_AMOUNT cannot be negative,
Vendor 2 has 4 errorneous values
*/

--Checking the column tolls_amount
select vendorid,count(1) as records
from nyc_jd_raw
where tolls_amount < 0
group by vendorid
;

/*
Output:
vendorid	records
2			3

Inference:
TOLLS_AMOUNT cannot be negative,
Vendor 2 has 3 errorneous values
*/


--Checking the column total_amount
select vendorid,count(1) as records
from nyc_jd_raw
where total_amount < 0
group by vendorid
;

/*
Output:
vendorid	records
2			558

Inference:
total_amount cannot be negative,
Vendor 2 has 558 errorneous values
*/

/*
Assumptions after analysis : 
Passenger count can be zero for cancelled rides and if the taxi is transporting odd-goods.
Fare amount fields cannot be less than zero.
For the records that have drop off time on 01-01-2018, such records are considered valid provided the trip has started in the night of the last day of the year 2017.
*/

/*
Final Inference
Both Vendors have errorneous rows, but on comparison Vendor 2 ( Verifone Inc) has more error records than Vendor 1 ( Creative Mobile Technologies ) , hence Vendor 1 (Creative Mobile Technologies) has maintained better history of data.
*/	

------------------------------------------------
/*
Q.Before answering the below questions, you need to create a clean, ORC partitioned table for analysis. Remove all the erroneous rows.
*/

--Creating a partition table structure
SET hive.exec.max.dynamic.partitions=100000;
SET hive.exec.max.dynamic.partitions.pernode=100000;

create external table if not exists nyc_jd_raw_partition
(
VENDORID INT,
TPEP_PICKUP_DATETIME TIMESTAMP,
TPEP_DROPOFF_DATETIME TIMESTAMP,
PASSENGER_COUNT INT,
TRIP_DISTANCE DOUBLE,
RATECODEID INT,
STORE_AND_FWD_FLAG string,	
PULOCATIONID int,
DOLOCATIONID int,
PAYMENT_TYPE int,
FARE_AMOUNT double,
EXTRA double,
MTA_TAX double,
TIP_AMOUNT double,
TOLLS_AMOUNT double,
IMPROVEMENT_SURCHARGE double,
TOTAL_AMOUNT double
)
partitioned by (yr int, mnth int)
location '/user/hive/warehouse/jd';
-- Then insert the valid data in the table using partition month and year of the column TPEP_PICKUP_DATETIME i.e pickup time column

insert overwrite table nyc_jd_raw_partition partition(yr, mnth)
select VENDORID,
TPEP_PICKUP_DATETIME,
TPEP_DROPOFF_DATETIME,
PASSENGER_COUNT,
TRIP_DISTANCE,
RATECODEID,
STORE_AND_FWD_FLAG,
PULOCATIONID,
DOLOCATIONID,
PAYMENT_TYPE,
FARE_AMOUNT,
EXTRA,
MTA_TAX,
TIP_AMOUNT,
TOLLS_AMOUNT,
IMPROVEMENT_SURCHARGE,
TOTAL_AMOUNT,
 year(TPEP_PICKUP_DATETIME) as yr, month(TPEP_PICKUP_DATETIME) as mnth
from nyc_jd_raw
where
year(TPEP_PICKUP_DATETIME) = '2017'
and
month(TPEP_PICKUP_DATETIME) in ('11','12')
and 
year(TPEP_DROPOFF_DATETIME) in ('2017','2018')
and 
month(TPEP_DROPOFF_DATETIME) in ('11','12','1')
and
ratecodeid in (1,2,3,4,5,6)
and
fare_amount >=0
and
extra >=0
and
mta_tax in (0,0.5)
and
Improvement_surcharge in (0,0.3)
and
tip_amount >= 0
and
tolls_amount >= 0
and
total_amount >= 0
;
--Valid Records have inserted.
--Checking the count
select * from nyc_jd_raw_partition;
--Count : 1173982

-- ORC FILE FORMAT 
-- This format improves query performance 
-- Creating an ORC table for analysis
 
create external table if not exists nyc_jd_raw_partition_orc
(
VENDORID INT,
TPEP_PICKUP_DATETIME TIMESTAMP,
TPEP_DROPOFF_DATETIME TIMESTAMP,
PASSENGER_COUNT INT,
TRIP_DISTANCE DOUBLE,
RATECODEID INT,
STORE_AND_FWD_FLAG string,	
PULOCATIONID int,
DOLOCATIONID int,
PAYMENT_TYPE int,
FARE_AMOUNT double,
EXTRA double,
MTA_TAX double,
TIP_AMOUNT double,
TOLLS_AMOUNT double,
IMPROVEMENT_SURCHARGE double,
TOTAL_AMOUNT double
)
partitioned by (yr int, mnth int)
stored as orc location '/user/hive/warehouse/jd'
tblproperties ("orc.compress"="SNAPPY"); 


-- Then, write data from partition table into ORC table 

insert overwrite table nyc_jd_raw_partition_orc partition(yr , mnth)
select * from nyc_jd_raw_partition;

--Checking the count of orc table created
select count(1) from nyc_jd_raw_partition_orc;
--1173982
 
--The  table to do analysis is now ready


----------------------------------------------------

--Analysis-I
/*Q1.
Compare the overall average fare per trip for November and December.
*/
--Solution : 
select
mnth,round(avg(fare_amount),2) as avg_fare_amount
from 
nyc_jd_raw_partition_orc 
where yr='2017' and mnth in ('11','12')
group by mnth

/*
Output

mnth	avg_fare_amount
12		12.9
11		13.11

Inference : 
Month of Novemeber has more average fare amount
*/


/*Q2.
Explore the ‘number of passengers per trip’ - how many trips are made by each level of ‘Passenger_count’? Do most people travel solo or with other people?
*/

--Solution :
select
passenger_count,count(1) as records
from 
nyc_jd_raw_partition_orc 
group by 
passenger_count
order by records desc;

/*
Output:

passenger_count	|	Records
1				|	827144
2				|	176767
5				|	54519
3				|	50662
6				|	33116
4				|	24940
0				|	6818
7				|	12
8				|	3
9				|	1

Inference: 

Most people travel solo rather than with others

*/

/*Q3.
Which is the most preferred mode of payment?
*/

--Solution :
select
payment_type,count(1) as records
from 
nyc_jd_raw_partition_orc 
group by 
payment_type
order by records desc;

/*
Output:
payment_type	records
1				790239
2				376362
3				5860
4				1521

Inference: 

Most preferred mode of payment is payment_type 1 which is 'Credit Card' as given in the data dictionary.

*/


/*Q4.
What is the average tip paid per trip? Compare the average tip with the 25th, 50th and 75th percentiles and comment whether the ‘average tip’ is a representative statistic (of the central tendency) of ‘tip amount paid’. Hint: You may use percentile_approx(DOUBLE col, p): Returns an approximate pth percentile of a numeric column (including floating point types) in the group.
*/

--Solution

--Finding average tip amount

select round(avg(tip_amount),2) as avg_tip_amount
 from 
nyc_jd_raw_partition_orc ;

--Output --> 1.85


--Finding the 25th,50th,75th percentile


select
round(percentile_approx(tip_amount, 0.25),2) as percentile_25,
round(percentile_approx(tip_amount, 0.50),2) as percentile_50,
round(percentile_approx(tip_amount, 0.75),2) as percentile_75
 from 
nyc_jd_raw_partition_orc ;

/*Output


percentile_25 | percentile_50 	 |	percentile_75
0			  | 1.35		     |	2.45



Inference: 
Average is not a very representative statistic as it does not match with the 50th percentile
*/

/*
Q5.
Explore the ‘Extra’ (charge) variable - what fraction of total trips have an extra charge is levied?
*/

--Solution :
select
round(sum(case when extra > 0 then 1 else 0 end ) /count(*),2)
as fraction_extra,
round((sum(case when extra > 0 then 1 else 0 end ) /count(*))*100,2)
as percentage_extra
from
nyc_jd_raw_partition_orc


/*Output:

fraction_extra	|	percentage_extra
0.46			|	46.2

Inference : 
Fraction where extra charge is levied is 0.46,
extra charge is levied in 46.2 % of the cases.

*/

----------------------------------------------------
--Analysis-II

/*Q1.
What is the correlation between the number of passengers on any given trip, and the tip paid per trip? Do multiple travellers tip more compared to solo travellers? Hint: Use CORR(Col_1, Col_2)
*/

--Solution:

select
CORR(passenger_Count, tip_amount) as correlation
from
nyc_jd_raw_partition_orc

/*Output
Correlation

0.0042802449225309855

Inference

There is hardly any correlation between passenger_count and tip amount.


/*Q2.
Segregate the data into five segments of ‘tip paid’: [0-5), [5-10), [10-15) , [15-20) and >=20. Calculate the percentage share of each bucket (i.e. the fraction of trips falling in each bucket).
*/


--Solution:


with cnt
as (select count(1) cnt from nyc_jd_raw_partition_orc )

select 
s.tip_bucket as tip_bucket,round((s.bucket_count/cnt.cnt)*100,2) as percentage
from
(
select
case 
when tip_amount>= 0 and tip_amount < 5 then '0-5'
when tip_amount>= 5 and tip_amount < 10 then '5-10'
when tip_amount>= 10 and tip_amount < 15 then '10-15'
when tip_amount>= 15 and tip_amount < 20 then '15-20'
when tip_amount>=20 then '20' end as tip_bucket,
count(1) as bucket_count
from
nyc_jd_raw_partition_orc
group by 
case 
when tip_amount>= 0 and tip_amount < 5 then '0-5'
when tip_amount>= 5 and tip_amount < 10 then '5-10'
when tip_amount>= 10 and tip_amount < 15 then '10-15'
when tip_amount>= 15 and tip_amount < 20 then '15-20'
when tip_amount>=20 then '20' end
) s
inner join cnt on (1=1)
order by percentage desc;

/*Output

tip_bucket	|	percentage
0-5			|	92.12
5-10		|	5.65
10-15		|	1.89
15-20		|	0.24
20			|	0.11

Inference:

[0-5] is the best common bucket of tips.

*/

/*Q3.
Which month has a greater average ‘speed’ - November or December? Note that the variable ‘speed’ will have to be derived from other metrics. Hint: You have columns for distance and time.
*/

--Solution:

select mnth, avg(trip_distance/(unix_timestamp(tpep_dropoff_datetime)- unix_timestamp(tpep_pickup_datetime))/3600) as avg_speed
from nyc_jd_raw_partition_orc
where
tpep_dropoff_datetime >= tpep_pickup_datetime
group by mnth;


/*Output

mnth	avg_speed
11		8.471711268575636e-07
12		8.543122682285371e-07

Inference:
Month of December has greater average speed of 8.54 miles/hour
*/


/*Q4.
Analyse the average speed of the most happening days of the year, i.e. 31st December (New year’s eve) and 25th December (Christmas) and compare it with the overall average. 
*/

--Solution
select Day_type,
round(avg(trip_distance/((unix_timestamp(tpep_dropoff_datetime)-unix_timestamp(tpep_pickup_datetime) )/3600)),2) avg_speed
from 
( 
select 
trip_distance,
tpep_dropoff_datetime,
tpep_pickup_datetime,
case when (tpep_pickup_datetime>='2017-12-25 00:00:00.0' and tpep_pickup_datetime<'2017-12-26 00:00:00.0') then 'Christmas'
when (tpep_pickup_datetime>='2017-12-31 00:00:00.0') then 'New_Year'
 else 'Other_days' end Day_type 
from nyc_jd_raw_partition_orc
) s
group by Day_type
order by avg_speed desc;

/*Output:

day_type	avg_speed
Christmas	15.23
New_Year	13.2
Other_days	10.96


Inference : 
Speeds on holidays like Christmas and New Year are more due to less traffic compared to other days.

----------------------END---------------------------
