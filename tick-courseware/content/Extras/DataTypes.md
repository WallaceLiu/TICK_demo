# Data Types

* Measurements, tag keys, tag values, and field keys are always strings
* InfluxDB cannot perform math on tag values
* InfluxQL functions do not accept a tag value as a primary argument
* Timestamps are UNIX timestamps in nanoseconds
* **Float**: InfluxDB assumes all numerical field values are floats
* **Integer**: ```i``` to the right of field value to store it as an integer:
```
butterflies=12i,honeybees=23i
```
* **String***: double quote string field values:
```
scientist="langstroth"
```
* **Boolean**: t, T, true, True, TRUE vs. f, F, false, False, FALSE
* Within a measurement shard field type cannot differ
* Field type between shards can differ