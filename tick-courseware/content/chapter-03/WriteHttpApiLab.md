# Http Api Lab

* Login into your sandbox instance
* Create new database using <a href="https://docs.influxdata.com/influxdb/v1.3/guides/writing_data/#creating-a-database-using-the-http-api" target="_blank">Http Api</a>
* Using ```influx``` cli confirm a database has been created before proceeding to the next steps: ```show databases```
* Write some data using line protocol and <a href="https://docs.influxdata.com/influxdb/v1.3/guides/writing_data/#writing-data-using-the-http-api" target="_blank">Http Api</a>
* Write multi-line data using <a href="https://docs.influxdata.com/influxdb/v1.3/guides/writing_data/#writing-multiple-points" target="_blank">Http Api</a>
* Use cli to confirm the data base been written: 
```select * from <db-name>.<measurement name> limit 10```
* Create a new retention policy using Http Api
* Create a new continuous query using Http Api
* Write data to non-default retention policy