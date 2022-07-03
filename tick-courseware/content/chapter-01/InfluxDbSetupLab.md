# InfluxDb Setup Lab

* Download and install deb package:
```
curl -O https://dl.influxdata.com/influxdb/releases/influxdb_1.3.4_amd64.deb
sudo dpkg -i influxdb_1.3.4_amd64.deb
```
* Start InfluxDb service and check status:
```
sudo systemctl start influxdb
sudo systemctl status influxdb
```
* Expected Output:
```
influxdb.service - InfluxDB is an open-source, distributed, time series database
   Loaded: loaded (/lib/systemd/system/influxdb.service; enabled; vendor preset: enabled)
   Active: active (running) since Mon 2017-08-28 21:04:51 UTC; 28min ago
     Docs: https://docs.influxdata.com/influxdb/
 Main PID: 2690 (influxd)
    Tasks: 10
   Memory: 12.5M
      CPU: 5.962s
   CGroup: /system.slice/influxdb.service
           └─2690 /usr/bin/influxd -config /etc/influxdb/influxdb.conf
           
<more lines>
```
* Where is configuration file?
* What port the process is listening on?
* What version am I running?
* InfluxDb daemon cli: run ```influxd``` from terminal window
* Detailed configurations: run ```influxd condifg``` from terminal window
* How do I interact with InfluxDb?
* InfluxDb cli: run ```influx``` from terminal window
* Inside influx cli try:
```
help
show databases
```
* Creating and using your first database using influx cli:
```
create database fliesbees
use fliesbees
```
* Data to populate:

| time | butterflies | honeybees | location | scientist |
| --- | ---: | ---: | :---: | :--- |
| 2015-08-18T00:00:00Z | 12 | 23 | 1 | langstroth |

* influx cli syntax:
```
insert observations,location=1,scientist=langstroth butterflies=12,honeybees=23 1439856000
insert observations,location=2,scientist=perpetua butterflies=12,honeybees=23 1439856000
```
* For writes timestamp uses nanosecond-precision Unix time, not milliseconds and not seconds, <a href="https://repl.it/Kb8P/3" target="_blank">here is a converter</a>
* Retrieving data:
```
select * from observations
```
* Querying data:
```
select * from observations where time=1439856000
select * from observations where time > '2000-01-01'
select * from observations where location='1'
select * from observations where honeybees=23
```
* What are the differences between query by tags vs by field values
* Deleting data:
```
delete * from observations
```
* Type ```help``` inside influx cli to find more command including a reference to <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/spec/" target="_blank">InfluxQL Reference</a>
* How do we:
  * List all databases
  * List of measurements in a database
  * List fields keys
  * List tag keys
* <a href="https://docs.influxdata.com/influxdb/v1.3/query_language/schema_exploration/" target="_blank">Documentation for schema exploration</a>