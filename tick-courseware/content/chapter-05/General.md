# General Schema Guidance

* Fields are not indexed and assumed to be floats
* For other data types use literals or quotes e.g.: ```82i``` or ```'string value'```
* InfluxQl Functions can be applied on fields only
* Comparison operators can be applied on fields only
* Tags are strings and all are indexed
* Store meta-data in tags to improve query performance
* Store data in tags if you plan to use group-by statements
* Maybe, store the same data twice as a tag and as a field under different names
* Tag and field may share the same name creating an awkward InfluxQl syntax:
  ```
  select host::tag, host::field from cpu
  ```
 * Refrain from using InfluxQl keywords to avoid double quotes