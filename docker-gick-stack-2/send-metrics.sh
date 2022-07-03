#!/bin/bash

INPUTNUM=$1
OUTPUTNUM=$2
PAYLOAD_FILE="batch.json"
if [ "${RSOURCES:-false}" = "true" ]; then
  PAYLOAD_FILE="batch-sources.json"
fi

dir=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )

getJson () {
    sed "s/ANON_ID/$(echo $RANDOM | md5 | head -c 20)/" ${PAYLOAD_FILE}
}
doPost () {
  count=$1
  direction=$2
  curl -i -XPOST 'http://localhost:9992/write?db=metrics' --data-binary "jobsdb_events,dir=$direction value=$count"
}


doPost $INPUTNUM "in" &
if [ "$OUTPUTNUM" -gt "0" ]; then
  doPost $OUTPUTNUM "out" &    
fi


wait
