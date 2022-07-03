# CQ & RP
* query turns all tags into fields, to retain tags use group by
* cq common issues with internal time buckets potentially conflicting with time where close
* offset syntax in group by (1h, 15m) where 15 min is an offset
* http api += rp parameters
* use database now has an option to specify rp
* http api fully qualified db.rp.measurement for reading

# General
* rebalancing for enterprise edition

# Subquery
* subquery syntax idea for demo: mean tide point on hourly basis -> max tide point between locations -> higher average water level between locations

# Line protocol
* InfluxDb assumes all fields are floats, add i literal to save as an integer
* Absence of timestamp may put events out of order as in case of race conditions

# Schema don't do
* many unique tag values
* field name and tag name the same
* fields cannot be used for grouping
* too few tags for the same timestamp - data overwrite
* writing data at higher level of precision that is required for analysis
* writing data at lower level of precision that required, e.g. at nanosecond when second only is required
* do not create too many databases/retention policies, dozens is great, hundreds is ok, thousands is a sign of a trouble
* influxdb may cause troubles to the OS due to too many files open, too much data in RAM
* expiring shards is a non-trivial cpu cost
* no support for regular expression to query across databases, unlike measurements but even the latter is not as performant

# Schema do do
* Think about questions that need to be ansered
* Functions can be applied on fields only
* Group by can be applied on tags only
* Tags stored as as string
* Comparison operators can be used on fields only
* RegEx can be applied on tags only
* lon/lat not great as tags too high of granularity, geo-hash and/or geohash prefix can be stored as a tag.
* lon/lat stored as tags improves query and group0by - maybe reduce precision before storing as a tag
* Model the queries to decide on what's a tag and what's a field

# Hardware Requirements
* run on ssd
* storage 1B points ~3GB of storage for non-string fields
* tag keys and names are stored once per series
* 

