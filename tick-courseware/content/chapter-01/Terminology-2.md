# Terminology cont'd

* **Measurement:** a container for tags, fields, and the time column - similar to a table in rdbms
* **Retention policy:** defintion for number of copies of data in a cluster and duration for InfluxDb to keep data before auto-eviction
* **Database:** logical container for time series data, users, retention policies, and continuous queries
* **Series:** definition for Measurement, Retention Policy, and Tag Set
* **Server:** physical server, virtual machine, or a container running InfluxDb process
* **Node:** InfluxDb process running on a server, one process per server
* **Cluster:** proprietary (closed source) clustering functionality to support fault-tolerance and scalability of the database and the stream-processor