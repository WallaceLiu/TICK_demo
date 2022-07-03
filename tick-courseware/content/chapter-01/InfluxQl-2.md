# InfluxQl vs. Sql - Cont'd

* **compound** clause:
```
  select * from observations where time > '2000-01-01' AND location='1'
```
* **delete**:
```
  delete from "observations"
  delete from "observations" where time > '2000-01-01'
```
* **group by**:
```
  select mean("butterflies") from "observations" group by "location"
  name: observations
  # Results
  tags: location=1
  time mean
  ---- ----
  0    12
```