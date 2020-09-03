
--load geolocation data

geolocation = LOAD '/tmp/data/geolocation' using PigStorage(',') AS(truckid: chararray, driverid: chararray, event: chararray, latitude: double, longitude:double, 
  city: chararray, state: chararray, velocity: chararray, event_ind: int, idling_ind: int);

-- select city, event,
eventBycity = FOREACH geolocation GENERATE truckid, city, event;

--group by city, event

grouped = GROUP eventBycity By (city, event);

-- calcuate the total numbe of events by city and event type

totalEvents = FOREACH grouped GENERATE FLATTEN(group) as (city, event), COUNT(eventBycity.truckid) as NumberofEvents;

-- order the event on decending order

ordered = Order totalEvents by NumberofEvents desc; 

-- limit to top 10
top10 = LIMIT ordered 10;

-- dump top10;

-- store the results to HDFS directory

STORE top10 INTO '/tmp/data/output_top10' using PigStorage('|');
