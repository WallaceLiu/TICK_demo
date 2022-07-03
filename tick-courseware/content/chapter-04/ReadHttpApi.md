# Read Http Api

* InfluxDB uses HTTP solely as a convenient and widely supported data transfer protocol
* Modern web APIs have settled on REST because it addresses a common need
* REST is the industry agreed style for organizing large numbers of endpoints
* **InfluxDB API makes no attempt to be RESTful**
* Sample request:
```
curl -G 'http://localhost:8086/query?pretty=true&db=NOAA_water_database' \
  --data-urlencode "q=SELECT * FROM h2o_feet WHERE location='coyote_creek' limit 2"
```
* Success response:
```
{
    "results": [
        {
            "statement_id": 0,
            "series": [
                {
                    "name": "h2o_feet",
                    "columns": [
                        "time",
                        "level description",
                        "location",
                        "water_level"
                    ],
                    "values": [
                        [
                            "2015-08-18T00:00:00Z",
                            "between 6 and 9 feet",
                            "coyote_creek",
                            8.12
                        ],
                        [
                            "2015-08-18T00:06:00Z",
                            "between 6 and 9 feet",
                            "coyote_creek",
                            8.005
                        ]
                    ]
                }
            ]
        }
    ]
}
```
