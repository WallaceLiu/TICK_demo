# Terminology by Example

* **Sample Data:** number of butterflies and honeybees counted by two scientists (langstroth and perpetua) at two locations (location 1 and location 2)

---
**name**: census

| time | butterflies | honeybees | location | scientist |
| --- | ---: | ---: | :---: | :--- |
| 2015-08-18T00:00:00Z | 12 | 23 | 1 | langstroth |
| 2015-08-18T00:00:00Z | 1 | 30 | 1 | perpetua |
| 2015-08-18T00:06:00Z | 11 | 28 | 1 | langstroth
| ... | ... | ... | ... | ...
___
* **census**: measurement, acts as a container for tags, fields, and the time column
* **time**: timestamp field, all data in InfluxDB have that column using <a href="https://www.ietf.org/rfc/rfc3339.txt" target="_blank">RFC3339</a> <a href="https://www.timeanddate.com/worldclock/timezone/utc" target="_blank">UTC</a> format
* **butterflies** and **honeybees** are field keys - the metadata
* numbers between **1** and **30** are field values - the data
* field-keys and field-values form **field set**, e.g.: ```butterflies = 12, honeybees = 23```
* **fields** are required and are not indexed, queries on fields scan entire dataset
* **location** and **scientist** are *optional* tag keys
* location **1** and **2**; scientists **langstroth** and **perpetua** are tag values
* tag-keys and tag-values from **tag key-value pairs**, e.g.: ```location = 1, scientist = langstroth```
* **tags** are indexed and hence queries on tags are faster than queries on fields