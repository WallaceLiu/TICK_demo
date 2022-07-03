#!/bin/sh
while :
	do curl -i -XPOST 'http://localhost:8086/write?db=telegraf' --data-binary 'disk,host=psuedo,path=test total=1000i,used=100i' > /dev/null
	sleep 10s
done
