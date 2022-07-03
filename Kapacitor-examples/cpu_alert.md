versions

```
kapacitor-1.0.2-1.x86_64
influxdb-0.13.0-1.x86_64
telegraf-1.0.1-1.x86_64
```

# testing the alert

Using database _telegraf_ with retention policy _default_::


```
kapacitor define cpu_alert   -type stream   -tick cpu_alert.tick   -dbrp telegraf.default
kapacitor record stream -task cpu_alert -duration 20s
# after the command ends, a notification id will be returned. 
# use it like in the following example.
rid=3f2467ed-f568-48f3-af55-aebfef1b76c4
kapacitor replay -recording $rid -task cpu_alert
```


# Monitoring cpu usage

## cpu\_alert.tick

```
# first two steps not really necesary unless the alert already exists and is being recreated
kapacitor disable cpu_alert
kapacitor delete tasks cpu_alert
# defining and enabling the alert in kapacitor
kapacitor define cpu_alert   -type stream   -tick cpu_alert.tick   -dbrp telegraf.default
kapacitor enable cpu_alert
```

Forcing the alerting:

Run the following on any of the monitored servers being used for the test.

```
# plan a
# to stress the cpu
for i in 1 2 3 4; do while : ; do : ; done & done
# to kill the process
for i in 1 2 3 4; do kill %$i; done

# plan b
# to stress the cpu
openssl speed -multi $(cat /proc/cpuinfo | grep processor | wc -l)
```

Example output:

```
[root@acme ~]# tail -f /tmp/cpu_alert_log.txt  | cut -d: -f5
host=ads.acme.com is INFO","details"
host=ads.acme.com is CRITICAL","details"
host=ads.acme.com is CRITICAL","details"
host=ads.acme.com is INFO","details"
host=ads.acme.com is OK","details"
^C
```

# Dead man switch

Sometime it's useful to know when the data stops being sent. That's when this feature can be used.

## cpu_alert_deadman.tick


```
kapacitor define cpu_alert_deadman   -type stream   -tick cpu_alert_deadman.tick   -dbrp telegraf.default
kapacitor enable cpu_alert_deadman

```

Example output:

```
"id":"node 'mean3' in task 'cpu_alert_deadman'","message":"host: ads.acme.com ip: ","details":"{\u0026#34;Name\u0026#34;:\u0026#34;stats\u0026#34;,\u0026#34;TaskName\u0026#34;:\u0026#34;cpu_alert_deadman\u0026#34;,\u0026#34;Group\u0026#34;:\u0026#34;host=ads.acme.com\u0026#34;,\u0026#34;Tags\u0026#34;:{\u0026#34;host\u0026#34;:\u0026#34;ads.acme.com\u0026#34;},\u0026#34;ID\u0026#34;:\u0026#34;node \u0026#39;mean3\u0026#39; in task \u0026#39;cpu_alert_deadman\u0026#39;\u0026#34;,\u0026#34;Fields\u0026#34;:{\u0026#34;emitted\u0026#34;:0},\u0026#34;Level\u0026#34;:\u0026#34;CRITICAL\u0026#34;,\u0026#34;Time\u0026#34;:\u0026#34;2016-10-28T00:08:30Z\u0026#34;,\u0026#34;Message\u0026#34;:\u0026#34;host: ads.acme.com ip: \u0026#34;}\n","time":"2016-10-28T00:08:30Z","duration":90000000000,"level":"CRITICAL","data":{"series":[{"name":"stats","tags":{"host":"ads.acme.com"},"columns":["time","emitted"],"values":[["2016-10-28T00:08:30Z",0]]}]}}
{"id":"node 'mean3' in task 'cpu_alert_deadman'","message":"host: ads.acme.com ip: ","details":"{\u0026#34;Name\u0026#34;:\u0026#34;stats\u0026#34;,\u0026#34;TaskName\u0026#34;:\u0026#34;cpu_alert_deadman\u0026#34;,\u0026#34;Group\u0026#34;:\u0026#34;host=ads.acme.com\u0026#34;,\u0026#34;Tags\u0026#34;:{\u0026#34;host\u0026#34;:\u0026#34;ads.acme.com\u0026#34;},\u0026#34;ID\u0026#34;:\u0026#34;node \u0026#39;mean3\u0026#39; in task \u0026#39;cpu_alert_deadman\u0026#39;\u0026#34;,\u0026#34;Fields\u0026#34;:{\u0026#34;emitted\u0026#34;:3},\u0026#34;Level\u0026#34;:\u0026#34;OK\u0026#34;,\u0026#34;Time\u0026#34;:\u0026#34;2016-10-28T00:09:00Z\u0026#34;,\u0026#34;Message\u0026#34;:\u0026#34;host: ads.acme.com ip: \u0026#34;}\n","time":"2016-10-28T00:09:00Z","duration":120000000000,"level":"OK","data":{"series":[{"name":"stats","tags":{"host":"ads.acme.com"},"columns":["time","emitted"],"values":[["2016-10-28T00:09:00Z",3]]}]}}
```

# Troubleshooting

At startup, kapacitor creates the required subscriptions. Check in the log for those messages.

Also check influx's subscriptions:

```
netstat -ntapul | grep kapacitord && influx -execute 'show subscriptions'
```

Check kapacitor is receiving data from influx.

```
kapacitor stats ingress
```

# Maintence 

Deleting unused recordings

```
for i in `kapacitor list recordings | grep stream | awk '{print $1}'` ; do kapacitor delete recordings $i ; done
```



# TODO

* configure alerts
* format email messages
* add IM notifications (slack/telegram)
