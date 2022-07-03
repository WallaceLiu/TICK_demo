# Write Http Api

* InfluxDB uses HTTP solely as a convenient and widely supported data transfer protocol
* Modern web APIs have settled on REST because it addresses a common need
* REST is the industry agreed style for organizing large numbers of endpoints
* InfluxDB makes do with few API endpoints as a transfer method for InfluxQL:
```
/write
/query
/ping
/debug/requests
```
* **InfluxDB API makes no attempt to be RESTful**
* Sample request:
```
curl -i -XPOST 'http://localhost:8086/write?db=NOAA_water_database' \
  --data-binary 'h2o_feet,location=coyote_creek,level\ description=between\ 6\ and\ 9\ feet water_level=8.12'
```
* Success response:
```
HTTP/1.1 204 No Content
Content-Type: application/json
Request-Id: c57f0ef2-c654-11e7-80fd-000000000000
X-Influxdb-Version: 1.3.4
Date: Fri, 10 Nov 2017 20:21:45 GMT
```
