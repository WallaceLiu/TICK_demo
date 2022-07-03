# Design Decisions

* <a href="https://docs.influxdata.com/influxdb/v1.3/concepts/crosswalk/#a-note-on-why-influxdb-isn-t-crud" target="_blank">InfluxDb is not CRUD database</a>
* Optimizing for time-series database resulted in some functionality dropped
* Timestamp is a unique identifier and data will be overwritten when it is duplicate
* Deletes and updates are rare and usually not performance important
* Data added with a recent timestamp and hence should be stored in a descending order
* Writing data with random timestamps distribution is not as performant 
* Scalable writes and reads are more important than always consistent data
* Most recent writes might not be available under load 
* It is NoSql database and there are no joins between "tables"
* There is not much of an importance for an individual data point and the points are differentiated by timestamp, not by ID

