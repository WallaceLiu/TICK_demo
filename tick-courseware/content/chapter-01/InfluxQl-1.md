# InfluxQl vs. Sql

* an sql-like query language for interacting with InfluxDB:
* **insert**:
```
insert observations,location=1,scientist=langstroth butterflies=12,honeybees=23 1439856000
```
* **select**:
```
select * from observations
select * from "observations"
```
* **where** clause is similar to sql:
```
select * from observations where time=1439856000
select * from observations where time > '2000-01-01'
# Does it use an index?
select * from observations where honeybees=23
```  
