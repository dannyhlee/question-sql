```
CREATE DATABASE Q2;

USE Q2;

/*********************************************************************/
-- define table for clickstream referrer totals for each page
/*********************************************************************/

CREATE TABLE CLICKSTREAM_REFERRER_TOTALS
    (REFERRER STRING,
    COUNT INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

DESCRIBE CLICKSTREAM_REFERRER_TOTALS;

/*********************************************************************/

LOAD DATA INPATH '/user/dannylee/q2data/clickstream-output' 
    INTO TABLE CLICKSTREAM_REFERRER_TOTALS;

/*********************************************************************/

SELECT * FROM CLICKSTREAM_REFERRER_TOTALS
ORDER BY REFERRER DESC
LIMIT 20;

/*********************************************************************/
-- define table for each pages visits
/*********************************************************************/

CREATE TABLE PAGECOUNT_TOTALS
    (PAGE_NAME STRING,
    COUNT INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

DESCRIBE PAGECOUNT_TOTALS;

/*********************************************************************/

LOAD DATA INPATH '/user/dannylee/q2data/pagecount-totalcounts' 
    INTO TABLE PAGECOUNT_TOTALS;

SELECT * FROM PAGECOUNT_TOTALS
ORDER BY PAGE_NAME DESC
LIMIT 20;

/*********************************************************************/
-- Get PAGECOUNT_TOTALS sorted by pagecount_totals
-- First get an idea of the max views for the month
/*********************************************************************/

SELECT * FROM PAGECOUNT_TOTALS
ORDER BY COUNT DESC
LIMIT 100;

/*********************************************************************/
-- Next, choose a cutoff number of views, so that low
-- view pages don't get 100% ratios.  Export to file.
/*********************************************************************/

INSERT OVERWRITE DIRECTORY '/user/hive/q2/'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT DISTINCT * FROM PAGECOUNT_TOTALS
WHERE COUNT > 50000;

/*********************************************************************/
-- rename file to 00_PAGECOUNT_TOTALS_OVER_50000
-- upload to hdfs /user/dannylee/q2data
/*********************************************************************/

-- IMPORT AS NEW TABLE from hdfs

CREATE TABLE PAGECOUNT_OVER_50000
    (PAGE_NAME STRING,
    COUNT INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

DESCRIBE PAGECOUNT_OVER_50000;

LOAD DATA INPATH '/user/dannylee/q2data/00_PAGECOUNT_TOTALS_OVER_50000' 
    INTO TABLE PAGECOUNT_OVER_50000;

SELECT * FROM PAGECOUNT_OVER_50000
ORDER BY COUNT DESC
LIMIT 100;

/*********************************************************************/
-- Get a count of entries
/*********************************************************************/

SELECT COUNT(*) FROM PAGECOUNT_OVER_50000;

/*********************************************************************/
-- Join the tables on PAGECOUNT_OVER_50000.page_name
--                    to CLICKSTREAM_REFERRER_TOTALS.referrer
-- output to file
/*********************************************************************/

INSERT OVERWRITE DIRECTORY '/user/hive/q2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT pc.page_name, pc.count, cs.count
FROM PAGECOUNT_OVER_50000 pc JOIN CLICKSTREAM_REFERRER_TOTALS cs
ON (pc.page_name = cs.referrer)
ORDER BY pc.page_name;

/*********************************************************************/
-- rename file to 00_PAGECOUNT_TO_REFERRER_OVER_50000
-- upload to hdfs /user/dannylee/q2data
/*********************************************************************/

/*********************************************************************/
-- IMPORT AS NEW TABLE from hdfs
/*********************************************************************/

CREATE TABLE PAGECOUNT_TO_REFERRER_OVER_50000
    (PAGE_NAME STRING,
    PAGECOUNT_COUNT INT,
    REFERRER_COUNT INT
    )
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

DESCRIBE PAGECOUNT_TO_REFERRER_OVER_50000;

LOAD DATA INPATH '/user/dannylee/q2data/00_PAGECOUNT_TO_REFERRER_OVER_50000' 
    INTO TABLE PAGECOUNT_TO_REFERRER_OVER_50000;

SELECT * FROM PAGECOUNT_TO_REFERRER_OVER_50000
ORDER BY PAGE_NAME ASC
LIMIT 100;

/*********************************************************************/
-- Get a count of entries
/*********************************************************************/

SELECT COUNT(*) FROM PAGECOUNT_TO_REFERRER_OVER_50000;

/*********************************************************************/
-- Calculate ratio of page views to referrals.
/*********************************************************************/

SELECT PAGE_NAME, REFERRER_COUNT, PAGECOUNT_COUNT, CAST((REFERRER_COUNT / PAGECOUNT_COUNT) as double) 
as REFERRAL_RATIO
FROM PAGECOUNT_TO_REFERRER_OVER_50000
ORDER BY REFERRAL_RATIO DESC
LIMIT 100;

/*********************************************************************/
-- Export as file for future reference
/*********************************************************************/

INSERT OVERWRITE DIRECTORY '/user/hive/q2'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
SELECT PAGE_NAME, REFERRER_COUNT, PAGECOUNT_COUNT, CAST((REFERRER_COUNT / PAGECOUNT_COUNT) as double) 
as REFERRAL_RATIO
FROM PAGECOUNT_TO_REFERRER_OVER_50000
ORDER BY REFERRAL_RATIO DESC;

```
