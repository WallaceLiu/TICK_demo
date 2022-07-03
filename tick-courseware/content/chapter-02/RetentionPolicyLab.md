# Retention Policy Lab

* Login into your sandbox instance
* Launch InfluxDb cli: ```influx``` from terminal window
* Drop existing database ```fliesbees```
* In case you forgot the <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/database_management/#delete-a-database-with-drop-database"  target="_blank">syntax</a>
* Recreate ```fliesbees``` datbase with a default retention policy:
```
create database fliesbees
use flies bees
create retention policy "ephemeral" on "fliesbees" duration 1h replication 1 default
```
* Try creating another retention policy with 1 minute duration, what's the syntax, what's the result?
* Display list of retention policies in the database, <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/schema_exploration" target="_blank">syntax link</a>
* Expected output:
```
name      duration shardGroupDuration replicaN default
----      -------- ------------------ -------- -------
autogen   0s       168h0m0s           1        false
ephemeral 1h0m0s   1h0m0s             1        true
```
* Let's write some data into the series using default retention policy:
```
insert observations,location=1,scientist=langstroth butterflies=12,honeybees=23 1439856000
insert observations,location=2,scientist=perpetua butterflies=12,honeybees=23 1439856000
```
* What is the result of the above statements?
* How do you write data using non-default retention policy?
* How do you read data using non-default retention policy?
* Psst: syntax is on the previous slide!
