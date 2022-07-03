#!/bin/sh
base=100
while :
	data=`echo "disk,host=psuedo,path=test total=1000i,used=${base}i"`
	echo $data
	do curl -i -XPOST 'http://localhost:8086/write?db=telegraf' --data-binary "$data" 
	sleep 10s
	base=`expr $base + 4`
done