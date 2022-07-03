# Quoting

* InfluxDB has rules about double and single quotes
* Line Protocol accepts double and single quotes for measurement names, tag keys, tag values, and field keys
* But, InfluxDb interprets quotes in names as identifiers not as a qualifiers, resulting in names:
```
  "tag name with a space"
  'field name with single quotes'
```
* Timestamp with any kind of quotes will result in a parsing error
* Timestamp in where clause using <a href="https://tools.ietf.org/html/rfc3339" target="_balnk">rfc3339</a> format can use single quote:
  ```
  select * from observations where time > '2000-01-01'
  ```
* Special characters need an escape character, e.g.:
  ```
  insert measurement,tag\ name\ with \space=foo field\,name \with \comma=1
  ```
* <a href="https://docs.influxdata.com/influxdb/v1.3//write_protocols/line_protocol_reference/#quoting" target="_blank">More confusing rules...</a>