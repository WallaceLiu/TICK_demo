# Write Http Api - Cont'd

* InfluxDb gets away with three response code series:
```
  2xx: E.g. 204 No Content, it was a success wirting the data
  4xx: Could not understand the request
  5xx: The system is overloaded or significantly impaired
```
* InfluxDb uses REST as a transfer protocol to InfluxDb not for the purpose of resource representation
* For multiple points request - separate line protocol by new line character:
```
curl -i -XPOST 'http://localhost:8086/write?db=NOAA_water_database' \
  --data-binary \
  'h2o_feet,location=coyote_creek,level\ description=between\ 6\ and\ 9\ feet water_level=8.12
  h2o_feet,location=santa_monica,level\ description=below\ 3\ feet water_level=2.064
  h2o_feet,location=coyote_creek,level\ description=between\ 6\ and\ 9\ feet water_level=8.005'
```
* For file-based import: 
```
curl -i -XPOST 'http://localhost:8086/write?db=NOAA_water_database' \
  --data-binary @NOAA_data.txt
```
* Format is similar to multi-line protocol request, but not the same:
```
h2o_feet,location=coyote_creek water_level=8.120,level\ description="between 6 and 9 feet" 1439856000
h2o_feet,location=coyote_creek water_level=8.005,level\ description="between 6 and 9 feet" 1439856360
h2o_feet,location=coyote_creek water_level=7.887,level\ description="between 6 and 9 feet" 1439856720
```
