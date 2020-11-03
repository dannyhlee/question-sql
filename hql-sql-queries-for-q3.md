```
CREATE DATABASE Q3;

USE Q3;

CREATE TABLE CLICKSTREAM
    (PREV STRING,
    CURR STRING,
    TYPE STRING,
    COUNT INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

LOAD DATA INPATH '/user/dannylee/q3data/clickstream-enwiki-2020-08.tsv' 
    INTO TABLE CLICKSTREAM;

/***********************************************
    Select where links are internal
***********************************************/

SELECT * FROM CLICKSTREAM
WHERE TYPE='link'
ORDER BY COUNT DESC
LIMIT 100;
    
/***********************************************
    Select where links are internal and 
    max value of COUNT
***********************************************/

SELECT PREV, CURR, COUNT 
FROM CLICKSTREAM
WHERE COUNT = (SELECT MAX(COUNT) FROM CLICKSTREAM)
AND TYPE='link';


/***********************************************
    Use CTE (common table expression) 
    https://docs.cloudera.com/runtime/7.2.1/using-hiveql/topics/hive_create_a_table_using_a_cte.html
    to query the MAX(COUNT) from 'link' type 
    entries.
***********************************************/

WITH query1 AS 
    (SELECT PREV, CURR, COUNT FROM CLICKSTREAM  WHERE TYPE='link')
    SELECT PREV, CURR, COUNT FROM query1 WHERE 
        COUNT = (SELECT MAX(COUNT) FROM query1)


-- CREATE A NEW TABLE THAT JUST HAS 'link' (INTERNAL) TYPES

CREATE TABLE CLICKSTREAM_INTERNAL_LINK_ONLY AS
SELECT * FROM CLICKSTREAM
WHERE TYPE='link'
ORDER BY COUNT DESC;


-- join to total views list
-- move q2 view table data to q3
use q2;
alter table <table> rename to q3.<table>


-- Join the tables on PAGECOUNT_OVER_50000.page_name
--                    to CLICKSTREAM_REFERRER_TOTALS.referrer
-- output to file
-- maybe not - can't have missing data, skip and get a new table
-- from Hotel_california referrals

CREATE TABLE PAGECOUNT_CLICKSTREAM_INTERNAL AS
SELECT pc.page_name, pc.count, cs.prev, cs.curr, cs.count
FROM PAGECOUNT_OVER_50000 pc JOIN CLICKSTREAM_INTERNAL cs
ON (pc.page_name = cs.referrer)
ORDER BY pc.page_name;


-- Get list of all articles that Hotel_california refers to 

CREATE TABLE CLICKSTREAM_HOTEL_CALIFORNIA AS
SELECT * FROM CLICKSTREAM_INTERNAL_LINK_ONLY 
WHERE PREV='Hotel_California';

-- Get the one (article1) that has the most references to links
-- Get the number of references too

WITH query1 AS 
    (SELECT PREV, CURR, COUNT FROM CLICKSTREAM  WHERE TYPE='link')
    SELECT PREV, CURR, COUNT FROM query1 WHERE 
        COUNT = (SELECT MAX(COUNT) FROM query1)
        
        
/***********************************************
    Use CTE (common table expression) for each article
    https://docs.cloudera.com/runtime/7.2.1/using-hiveql/topics/hive_create_a_table_using_a_cte.html
***********************************************/
       
CREATE TABLE LEVELM AS
SELECT PREV, CURR, COUNT FROM CLICKSTREAM_INTERNAL_LINK_ONLY 
WHERE PREV='Long_Road_Out_of_Eden';

-- Join level 1 with pc_totals and calculate ratio

CREATE TABLE LEVELM_JOINED_PC AS
SELECT lvl.curr, lvl.count, pc.page_name, pc.pc_count, 
    CAST((lvl.count/ pc.pc_count) as double) as ratio
FROM LEVELM lvl JOIN PC_TOTALS pc
ON (lvl.curr = pc.page_name)
ORDER BY ratio;

-- Select distinct, drop some columns and create new table to level1_results

CREATE TABLE LEVELM_RESULTS AS
select distinct page_name, count, pc_count, ratio 
FROM LEVELM_joined_pc
ORDER BY ratio DESC;

select * from LEVELM_RESULTS;



```








