# Continuous Query

* Looking at the sample data - we have a lot of individual measure points:
```
h2o_feet,location=coyote_creek water_level=8.120,level\ description="between 6 and 9 feet" 1439856000
h2o_feet,location=coyote_creek water_level=8.005,level\ description="between 6 and 9 feet" 1439856360
h2o_feet,location=coyote_creek water_level=7.887,level\ description="between 6 and 9 feet" 1439856720
h2o_feet,location=coyote_creek water_level=7.762,level\ description="between 6 and 9 feet" 1439857080
h2o_feet,location=coyote_creek water_level=7.635,level\ description="between 6 and 9 feet" 1439857440
h2o_feet,location=coyote_creek water_level=7.500,level\ description="between 6 and 9 feet" 1439857800
```
* Basic CQ syntax (multi-lines in cli are <a href="https://github.com/influxdata/influxdb/issues/4279" target="_blank">not supported</a>):
```
CREATE CONTINUOUS QUERY <cq_name> ON <database_name>
BEGIN
  SELECT <function[s]> INTO <destination_measurement> FROM <measurement> [WHERE <criteria>] GROUP BY time(<interval>)[,<tag_key[s]>]
END
```
* Example of CQ for NOAA data (eliminate line breaks!):
```
CREATE CONTINUOUS QUERY "cq-h2o-feet-daily" ON "NOAA_water_database"
BEGIN
  SELECT mean("water_level") INTO "h2o-feet-daily" FROM "h2o_feet" GROUP BY time(1d)
END
```
* CQ requires a function, an INTO clause, and a GROUP BY time() clause
* **function:** count(), distinct(), max(), top() and quite <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/functions/" target="_blank">a few more</a>
* **INTO:** name of the measurement to write results into
* **GROUP BY time():** timestamp interval e.g.: 12m or 1d, <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/data_exploration/#duration-literal" >more duration literals</a> 
* <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/data_exploration/#common-issues-with-basic-syntax" target="_blank">Common issues with basic syntax</a>
