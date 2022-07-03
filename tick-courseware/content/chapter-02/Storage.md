# InfluxDb Storage

* Over the releases InfluxDb has changed its <a href="https://docs.influxdata.com/influxdb/v0.8/advanced_topics/sharding_and_storage/" target="_blank">storage engine</a>
* Today v1.3 storage engine looks very similar to a LSM Tree
* <a href="http://paperhub.s3.amazonaws.com/18e91eb4db2114a06ea614f0384f2784.pdf" target="_blank">Log-Structured Merge-Tree</a>
* LSM Tree is designed to provide low-cost indexing for files experiencing a high rate of writes
* LSM Tree gives an advantage to a high update rate compared to a retrieval rate
* A common property for history tables and for log files
* <a href="https://docs.influxdata.com/influxdb/v1.3/concepts/storage_engine/" target="_blank">Detailed documentation on storage engines</a>