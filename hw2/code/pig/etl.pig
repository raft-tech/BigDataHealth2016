-- ***************************************************************************
-- TASK
-- Aggregate events into features of patient and generate training, testing data for mortality prediction.
-- Steps have been provided to guide you.
-- You can include as many intermediate steps as required to complete the calculations.
-- ***************************************************************************

-- ***************************************************************************
-- TESTS
-- To test, please change the LOAD path for events and mortality to ../../test/events.csv and ../../test/mortality.csv
-- 6 tests have been provided to test all the subparts in this exercise.
-- Manually compare the output of each test against the csv's in test/expected folder.
-- ***************************************************************************

-- register a python UDF for converting data into SVMLight format
REGISTER utils.py USING jython AS utils;

-- load events file
events = LOAD '../../data/events.csv' USING PigStorage(',') AS (patientid:int, eventid:chararray, eventdesc:chararray, timestamp:chararray, value:float);
-- events = LOAD '../test/events.csv' USING PigStorage(',') AS (patientid:int, eventid:chararray, eventdesc:chararray, timestamp:chararray, value:float);

-- select required columns from events
events = FOREACH events GENERATE patientid, eventid, ToDate(timestamp, 'yyyy-MM-dd') AS etimestamp, value;

-- load mortality file
mortality = LOAD '../../data/mortality.csv' USING PigStorage(',') as (patientid:int, timestamp:chararray, label:int);
-- mortality = LOAD '../test/mortality.csv' USING PigStorage(',') as (patientid:int, timestamp:chararray, label:int);

mortality = FOREACH mortality GENERATE patientid, ToDate(timestamp, 'yyyy-MM-dd') AS mtimestamp, label;

--To display the relation, use the dump command e.g. DUMP mortality;

-- ***************************************************************************
-- Compute the index dates for dead and alive patients
-- ***************************************************************************
eventswithmort = JOIN events by patientid LEFT, mortality by patientid;

-- detect the events of dead patients and create it of the 
deadevents = Filter eventswithmort by(mortality::patientid is not null);
-- describe deadevents;
-- form (patientid, eventid, value, label, time_difference) where time_difference is the days between index date and each event timestamp
-- describe deadevents;
deadevents = FOREACH deadevents GENERATE events::patientid as patientid, events::eventid as eventid, events::value as value, mortality::label as label, DaysBetween(mortality::mtimestamp,events::etimestamp)-30 as time_difference;


-- aliveevents = -- detect the events of alive patients and create it of the 
aliveevents = Filter eventswithmort by (mortality::patientid is null);
-- form (patientid, eventid, value, label, time_difference) where time_difference is the days between index date and each event timestamp
-- aliveevents = FOREACH aliveevents GENERATE 
-- describe aliveevents
aliveevents = FOREACH aliveevents GENERATE  events::patientid as patientid, events::eventid as eventid, events::value as value, 0 as label, events::etimestamp as timestamp;
alivegrp = GROUP aliveevents BY patientid;
aliveeventsindex = foreach alivegrp generate group as patientid,MAX(aliveevents.timestamp) as index;
aliveevents = join aliveevents by patientid, aliveeventsindex by patientid;
aliveevents =   FOREACH aliveevents GENERATE aliveevents::patientid as patientid, aliveevents::eventid as eventid, aliveevents::value as value, aliveevents::label as label, DaysBetween(aliveeventsindex::index,aliveevents::timestamp) as time_difference;
-- dump aliveevents


-- --TEST-1
-- deadevents = ORDER deadevents BY patientid, eventid;
-- aliveevents = ORDER aliveevents BY patientid, eventid;
-- STORE aliveevents INTO 'aliveevents' USING PigStorage(',');
-- STORE deadevents INTO 'deadevents' USING PigStorage(',');

-- -- ***************************************************************************
-- -- Filter events within the observation window and remove events with missing values
-- -- ***************************************************************************
dead= FILTER deadevents BY (time_difference<=2000 and time_difference>=0);
alive= FILTER aliveevents BY (time_difference<=2000 and time_difference>=0);
-- contains only events for all patients within the observation window of 2000 days and is of the form (patientid, eventid, value, label, time_difference)
filtered =union dead, alive;
filtered= filter filtered by (value is not null);
-- --TEST-2
-- filteredgrpd = GROUP filtered BY 1;
-- filtered = FOREACH filteredgrpd GENERATE FLATTEN(filtered);
-- filtered = ORDER filtered BY patientid, eventid,time_difference;
-- STORE filtered INTO 'filtered' USING PigStorage(',');

-- -- ***************************************************************************
-- -- Aggregate events to create features
-- -- ***************************************************************************
-- featureswithid = 

grp = group filtered by (patientid, eventid);
-- describe featuresgrp;
-- dump featuresgrp;
-- for group of (patientid, eventid), count the number of  events occurred for the patient and create relation 
-- of the form (patientid, eventid, featurevalue)
featureswithid = FOREACH grp GENERATE group.$0 as patientid, group.$1 as eventid, COUNT(filtered.$0) as featurevalue;


-- --TEST-3
-- featureswithid = ORDER featureswithid BY patientid, eventid;
-- STORE featureswithid INTO 'features_aggregate' USING PigStorage(',');

-- -- ***************************************************************************
-- -- Generate feature mapping
-- -- ***************************************************************************
-- all_features = -- compute the set of distinct eventids obtained from previous step, sort them by eventid and then rank these features by eventid to 
-- create (idx, eventid). Rank should start from 0.

-- compute the set of distinct eventids obtained from previous step, sort them by eventid and then rank these features by eventid to create (idx, eventid). Rank should start from 0.
all_features = foreach featureswithid generate eventid;
all_features= DISTINCT all_features;
all_features= Order all_features by eventid;
all_features = RANK all_features;

all_features = FOREACH all_features GENERATE $0-1 AS idx, $1;

-- -- store the features as an output file
STORE all_features INTO 'features' using PigStorage(' ');

-- perform join of featureswithid and all_features by eventid and replace eventid with idx. It is 
features = JOIN featureswithid BY eventid LEFT, all_features BY eventid;
-- of the form (patientid, idx, featurevalue)
features = FOREACH features GENERATE featureswithid::patientid as patientid, all_features::idx as idx, featureswithid::featurevalue as featurevalue;


-- --TEST-4
-- features = ORDER features BY patientid, idx;
-- STORE features INTO 'features_map' USING PigStorage(',');

-- -- ***************************************************************************
-- -- Normalize the values using min-max normalization
-- -- ***************************************************************************
-- maxvalues = -- group events by idx and compute the maximum feature value in each group. I t is of the form (idx, maxvalues)

features_idx = GROUP features by idx;
maxvalues = FOREACH features_idx GENERATE group as idx, MAX(features.featurevalue) as maxvalues;

normalized = join features by idx, maxvalues by idx; -- join features and maxvalues by idx

-- compute the final set of normalized features of the form (patientid, idx, normalizedfeaturevalue)
features = foreach normalized generate features::patientid as patientid, features::idx as idx, ((float)features::featurevalue/maxvalues::maxvalues) as normalizedfeaturevalue;

-- --TEST-5
features = ORDER features BY patientid, idx;
STORE features INTO 'features_normalized' USING PigStorage(',');

-- -- ***************************************************************************
-- -- Generate features in svmlight format
-- -- features is of the form (patientid, idx, normalizedfeaturevalue) and is the output of the previous step
-- -- e.g.  1,1,1.0
-- --  	 1,3,0.8
-- --	 2,1,0.5
-- --       3,3,1.0
-- -- ***************************************************************************

grpd = GROUP features BY patientid;
grpd_order = ORDER grpd BY $0;
features = FOREACH grpd_order
{
    sorted = ORDER features BY idx;
    generate group as patientid, utils.bag_to_svmlight(sorted) as sparsefeature;
}

-- -- ***************************************************************************
-- -- Split into train and test set
-- -- labels is of the form (patientid, label) and contains all patientids followed by label of 1 for dead and 0 for alive
-- -- e.g. 1,1
-- --	2,0
-- --      3,1
-- -- ***************************************************************************

labels_group = GROUP filtered by patientid;
-- create it of the form (patientid, label) for dead and alive patients
labels = FOREACH labels_group GENERATE group as patientid, MIN(filtered.label);

--Generate sparsefeature vector relation
samples = JOIN features BY patientid, labels BY patientid;
samples = DISTINCT samples PARALLEL 1;
samples = ORDER samples BY $0;
samples = FOREACH samples GENERATE $3 AS label, $1 AS sparsefeature;

--TEST-6
STORE samples INTO 'samples' USING PigStorage(' ');

-- randomly split data for training and testing
samples = FOREACH samples GENERATE RANDOM() as assignmentkey, *;
SPLIT samples INTO testing IF assignmentkey <= 0.20, training OTHERWISE;
training = FOREACH training GENERATE $1..;
testing = FOREACH testing GENERATE $1..;

-- save training and tesing data
STORE testing INTO 'testing' USING PigStorage(' ');
STORE training INTO 'training' USING PigStorage(' ');