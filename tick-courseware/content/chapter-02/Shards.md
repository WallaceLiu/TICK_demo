# Shards

* Shard is a horizontal partition of a database or a search engine
* Term is not unique to InfluxDb and is possibly the corner stone of distributed databases design
* InfluxDB stores data in shard groups organized by retention policy to store data by time range
* Length of that time interval is shard group duration for auto-purge purposes  
* Default shard group duration based on retention policy:

| RP Duration | Shard Group Duration |
| --- | --- |
| < 2 days | 1 hour |
| >= 2 days and <= 6 months | 1 day |
| > 6 months | 7 days |