# Encouraged Schema Design

* As with other NoSql database model the queries rather than data storage
* Model your queries to decide what's tag and what field: group-by vs. function and comparisons
* Query statement turns tags into fields and may overwrite non-unique data
* Add ```group by``` to your query to retain uniqueness
* Regular expressions can be applied on tags only
* Longitude and Latitude are a bit tricky: tags or fields
* Common trick: compute <a href="https://en.wikipedia.org/wiki/Geohash" target="_blank">geohash</a> of desired length and store as a tag