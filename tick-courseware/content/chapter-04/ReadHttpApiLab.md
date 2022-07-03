# Read Http Api Lab

* Login into your sandbox instance
* Using Read Htto Api to fetch some data:
```
curl -G 'http://localhost:8086/query?pretty=true&db=NOAA_water_database' \
  --data-urlencode "q=SELECT * FROM h2o_feet WHERE location='coyote_creek' limit 2"
```
* Using Read Http Api to list databases:
```
curl -G 'http://localhost:8086/query?pretty=true' \
  --data-urlencode "q=<leverage InfluxQl to display databases"
```
* Using Read Http Api to explore measurements, continuous queries, and retention policies:
```
curl -G 'http://localhost:8086/query?pretty=true&db=NOAA_water_database' \
  --data-urlencode "q=<InfluxQl>"
```
