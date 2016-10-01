-- ***************************************************************************
-- Loading Data:
-- create external table mapping for events.csv and mortality_events.csv

-- IMPORTANT NOTES:
-- You need to put events.csv and mortality.csv under hdfs directory
-- '/input/events/events.csv' and '/input/mortality/mortality.csv'
--
-- To do this, run the following commands for events.csv,
-- 1. sudo su - hdfs
-- 2. hdfs dfs -mkdir -p /input/events
-- 3. hdfs dfs -chown -R vagrant /input
-- 4. exit
-- 5. hdfs dfs -put /path/to/events.csv /input/events/
-- Follow the same steps 1 - 5 for mortality.csv, except that the path should be
-- '/input/mortality'
-- ***************************************************************************
-- create events table
DROP TABLE IF EXISTS events;
CREATE EXTERNAL TABLE events (
  patient_id STRING,
  event_id STRING,
  event_description STRING,
  time DATE,
  value DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/events';

-- create mortality events table
DROP TABLE IF EXISTS mortality;
CREATE EXTERNAL TABLE mortality (
  patient_id STRING,
  time DATE,
  label INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/input/mortality';

-- ******************************************************
-- Task 1:
-- By manipulating the above two tables,
-- generate two views for alive and dead patients' events
-- ******************************************************
-- find events for alive patients
DROP VIEW IF EXISTS alive_events;
CREATE VIEW alive_events
AS
SELECT e.patient_id, e.event_id, e.time
from events e LEFT OUTER JOIN mortality m
on e.patient_id = m.patient_id
where m.patient_id is null;
-- ***** your code below *****







-- find events for dead patients
DROP VIEW IF EXISTS dead_events;
CREATE VIEW dead_events
AS
SELECT e.patient_id, e.event_id, e.time
from events e left outer join mortality m
on e.patient_id = m.patient_id
where m.patient_id is not null;
-- ***** your code below *****







-- ************************************************
-- Task 2: Event count metrics
-- Compute average, min and max of event counts
-- for alive and dead patients respectively
-- ************************************************
-- alive patients
SELECT avg(event_count), min(event_count), max(event_count)
from (
  select count (*) as event_count
  from alive_events
  group by alive_events.patient_id
) patients;





-- dead patients
SELECT avg(event_count), min(event_count), max(event_count)
from (
  select count(*) as event_count
  from dead_events
  group by dead_events.patient_id
) patients;






-- ************************************************
-- Task 3: Encounter count metrics
-- Compute average, min and max of encounter counts
-- for alive and dead patients respectively
-- ************************************************
-- alive
SELECT avg(encounter_count), min(encounter_count), max(encounter_count)
from (
  select count(DISTINCT ae.time) as encounter_count
  from alive_events ae
  group by ae.patient_id
) patients;
-- ***** your code below *****





-- dead
SELECT avg(encounter_count), min(encounter_count), max(encounter_count)
from (
  select count(DISTINCT de.time) as encounter_count
  from dead_events de
  group by de.patient_id
) patients;
-- ***** your code below *****







-- ************************************************
-- Task 4: Record length metrics
-- Compute average, min and max of record lengths
-- for alive and dead patients respectively
-- ************************************************
-- alive
SELECT avg(record_length), min(record_length), max(record_length)
from (
  SELECT DATEDIFF(max(ae.time), min(ae.time)) AS record_length
  FROM alive_events ae
  GROUP BY ae.patient_id
) patients;
-- ***** your code below *****





-- dead
SELECT avg(record_length), min(record_length), max(record_length)
from (
  select datediff(max(de.time), min(de.time)) as record_length
  from dead_events de
  group by de.patient_id
) patients;
-- ***** your code below *****








-- *******************************************
-- Task 5: Common diag/lab/med
-- Compute the 5 most frequently occurring diag/lab/med
-- for alive and dead patients respectively
-- *******************************************
-- alive patients
---- diag
SELECT event_id, count(*) AS diag_count
FROM alive_events ae
Where ae.event_id like 'DIAG%'
group by ae.event_id
order by diag_count DESC
limit 5;


---- lab
SELECT event_id, count(*) AS lab_count
FROM alive_events ae
Where ae.event_id like 'LAB%'
group by ae.event_id
order by lab_count DESC
limit 5;


---- med
SELECT event_id, count(*) AS med_count
FROM alive_events ae
Where ae.event_id like 'DRUG%'
group by ae.event_id
order by med_count DESC
limit 5;




-- dead patients
---- diag
SELECT event_id, count(*) AS diag_count
FROM dead_events de
Where de.event_id like 'DIAG%'
group by de.event_id
order by diag_count DESC
limit 5;

-- ***** your code below *****


---- lab
SELECT event_id, count(*) AS lab_count
FROM dead_events de
Where de.event_id like 'LAB%'
group by de.event_id
order by lab_count DESC
limit 5;
-- ***** your code below *****


---- med
SELECT event_id, count(*) AS med_count
FROM dead_events de
Where de.event_id like 'DRUG%'
group by de.event_id
order by med_count DESC
limit 5;
-- ***** your code below *****
