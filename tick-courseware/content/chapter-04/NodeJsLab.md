# Node.js Client Library Lab

* Login into your sandbox instance
* Edit index.js file:
```
nano ./index.js
```
* Enhance previous code with read element:
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
])
.then(() => {
  client.query(`
    select * from  h2o_feet
    where location='coyote_creek'
    order by time desc
    limit 10`)
})
.then(result => {
  console.log(result)
})
.catch(err => {
  console.error(err);
})
```
* Run the code:
```
nodejs ./index.js
```
* Confirm the new record has been inserted:
* <a href="https://repl.it/@vkhazin/TimestampNanosecs" target="_blank">Timestamp converter</a>

