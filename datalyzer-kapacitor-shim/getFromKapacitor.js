let fetch = require("node-fetch");
let _ = require('underscore');
let server = ""
let port = ""
if(! process.env.DATALYZER_KAPACITOR_IP){
  console.log("Kapacitor IP not set in environment variables")
  process.exit(0)
} else {
  server = process.env.DATALYZER_KAPACITOR_IP
}

if(! process.env.DATALYZER_KAPACITOR_PORT){
  console.log("Kapacitor Port not set in environment variables")
  process.exit(0)
} else {
  port = process.env.DATALYZER_KAPACITOR_PORT
}


// make this function generic and accept generator, measurement and date params
module.exports.getFromKapacitor = function(generator, measurement, dateRange){
  return new Promise(function(resolve, reject){
    fetch(`http://${server}:${port}/kapacitor/v1/tasks/get-gaps/${measurement}/gaps`)
    .then((res) => res.json())
    .then(function(responseObj) {
      // let finalOutput be an array of objects which has all the gaps
      let finalOutput = []
      //save the current date so that we can find the difference to get the daterange param equal to the request
      //series.gapTime - currentDate <= dateRange
      let today = new Date()
      //series the first property of the responseObj which has 'n' json objects
      //{series : [{},{},{}]}
      _.each(responseObj.series, (series) => {
        // get each series
        // each series has name and tags attribute, the series which has gaps has name, tags, columns and values
        // check if the series object has values attribute if so there is atleast one gap then we will parse in
        if(series.values){
          // there could be one or more gaps iterate over series.values and get all gaps
          _.each(series.values, (gapData) => {
            // seriesGaps is a temp container to hold gaps which we push to finalOutput
            // gapdata is a array with 2 items [<time>, <elapsed>]
            let diffDays = parseInt((today - new Date(gapData[0])) / (1000 * 60 * 60 * 24))
            // the daterange is a string witnh 120d / 60d / 30d so we need to remove the 'd' and parse it as int
            dateRange = dateRange.toString().replace( /d/g, '')
            dateRange = parseInt(dateRange)

            if(series.tags.generator == generator && diffDays <= dateRange){
              //only push the result if the generator matches with the request object
              let seriesGaps = {}
              seriesGaps['elapsed'] = gapData[1]
              seriesGaps['time'] = gapData[0]
              seriesGaps['generator'] = series.tags.generator
              seriesGaps['number'] = series.tags.number
              seriesGaps['site'] = series.tags.site
              seriesGaps['units'] = series.tags.units
              seriesGaps['location'] = series.tags.location? series.tags.location : ''
              seriesGaps['method'] = series.tags.method? series.tags.method : ''
              seriesGaps['measurement'] = series.name
              finalOutput.push(seriesGaps)
            }
          })
        }
      })
      resolve(finalOutput)
    })
  })

}
