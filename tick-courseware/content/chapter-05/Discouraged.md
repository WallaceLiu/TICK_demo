# Discouraged Schema Design

* Don't encode data into a measurement name e.g.: server1_us_east_1_mysql
* Don't encode multiple data elements into one tag e.g.: building_1_rack1_slot2_redhat
* Too many unique values in a tag field - memory pressure
* Using the same name for a field and for a tag
* Too few tags at low level time resolution may result in data overwrites
* Recording data at higher time resolution than required - loss of precision
* Recording data at lower than required - resource consumption
* Too many databases, measurements and/or retention policies: dozens is great, hundreds is OK, thousands is a sign of trouble
* InfluxDb may crash under a memory pressure
* InfluxDb may impact OS performance e.g. due to number of open files
* Monitor your installation/cluster to detect troubles early