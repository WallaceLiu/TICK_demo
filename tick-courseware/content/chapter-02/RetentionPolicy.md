# Retention Policy

* How long InfluxDB should keep data
* What number of copies to store in a cluster (not meaningful in one-node cluster)
* Time range for shard groups
* Default Retention Policy ```autogen``` is created when database is created
* ```autogen``` comes with infinite duration, a replication factor of one, and a shard group duration of seven days
* Custom retention policy can be specified when creating a database:
```
create database "fliesbees" with duration 1m replication 1 shard duration 1s name "ephemeral"
```
* A new retention policy can be created in an existing database and set as a default:
```
create retention policy "ephemeral" on "fliesbees" duration 1h replication 1 default
```
* Retention policy duration:
  ```
  m minutes
  h hours
  d days
  w weeks
  INF infinite
  ```
* Writing data using non-default retention policy:
```
INSERT INTO [<database>.]<retention_policy> <line_protocol>
```
* Reading data using non-default retention policy:
```
select * from [<database>.]<retention_policy>.<measurement>
```
* Purge is not instant: InfluxDB checks to enforce an RP every 30 minutes 
