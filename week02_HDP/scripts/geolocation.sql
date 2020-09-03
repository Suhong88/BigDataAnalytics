-- go to port 4200. Log in with root (password hadoop).
-- execute hive in command line: hive -f filename 

CREATE EXTERNAL TABLE IF NOT EXISTS geolocation_02(
  truckid string, 
  driverid string, 
  event string, 
  latitude double, 
  longitude double, 
  city string, 
  state string, 
  velocity int, 
  event_ind int, 
  idling_ind int)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS ORC
LOCATION '/tmp/data/geolocation/';

INSERT OVERWRITE DIRECTORY '/tmp/data/output' 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
select city, event, count(*) as NumberOfEvents
from geolocation_02
where event!='normal'
group by city, event
order by city, event