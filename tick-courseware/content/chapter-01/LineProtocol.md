# Line Protocol

* Example from the previous chapter:
```
insert observations,location=1,scientist=langstroth butterflies=12,honeybees=23 1439856000
```
* Sql-like and Json-Like **Line Protocol**
* Single line of Line Protocol represents one data point in InfluxDB where
* **observations** is the name of measurement
* **location=1,scientist=langstroth** is the tag set, optional
* Why do we need a tag set?
* **white space** separates measurement, tag set, and field set
* **butterflies=12,honeybees=23** is the field set
* **1439856000** is the timestamp, if omitted system timestamp is used
