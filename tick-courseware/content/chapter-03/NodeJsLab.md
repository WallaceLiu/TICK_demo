# Node.js Client Library Lab

* Login into your sandbox instance
* Install Node.Js and package manager npm:
```
sudo apt-get install nodejs npm -y
```
* Check Node.Js and Npm have been installed 
```
npm --version
nodejs --version
```
* Create new folder for the project: ```mkdir ./nodejs-influxdb-sample```
* Initialize the project:
```
cd nodejs-influxdb-sample/
npm init
```
* Install client library:
```
npm install --save influx
```
* Create index.js file:
```
nano ./index.js
```
* Copy-paste start of the program:
```
client.writePoints([
  {
    measurement: 'response_times',
    tags: { 
      location: 'coyote_creek',
      "level description": 'between 6 and 9 feet'
    },
    fields: {
      water_level: 8.12
    }
  }
]).catch(err => {
  console.error(`Error saving data to InfluxDB! ${err.stack}`)
});
```
* Run the code:
```
nodejs ./index.js
```
* Confirm the new record has been inserted:
```
influx -database "NOAA_water_database" -execute "select * from h2o_feet order by time desc limit 1;"
```
* <a href="https://repl.it/@vkhazin/TimestampNanosecs" target="_blank">Timestamp converter</a>

