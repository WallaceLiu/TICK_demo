# tick-stack-chronograf

name|version|port|config file
---|---|---|---|
telegraf|1.11.0|-|/etc/telegraf/telegraf.conf
influxdb|1.7.6|8086 HTTP API|/etc/influxdb/influxdb.conf
-|-|8088 RPC
-|-|8083 web
chronograf|1.7.12|8888 web|-
kapacitor|1.5.2|9092 HTTP API|/etc/kapacitor/kapacitor.conf


1. 部署
```shell script
docker-compose up
```

2. 然后访问本地 chronograf web ui，地址 http://localhost:8888
在 chronograf 中设置 influxdb 和 kapacitor 连接字符串，连接字符串是它们的完全本地IP，例如：
- http://a.b.c.d:8086 for influxdb
- http://a.b.c.d:9092 for kapacitor

See https://www.youtube.com/watch?v=dk7ZdcNsrKE for a demo video

See https://vimeo.com/191737015 for a talk about this stack by Influx 


3. 设置infux
当你`docker-compose up`后，会看到界面输出，有错误信息：
```text
telegraf      | 2022-07-04T06:54:09Z E! [outputs.influxdb] when writing to [http://influxdb:8086]: 404 Not Found: database not found: "telegraf"
telegraf      | 2022-07-04T06:54:09Z E! [agent] Error writing to output [influxdb]: could not write any address
```
此时，按如下命令创建数据库，用户名和密码：
```shell script
# influx
Connected to http://localhost:8086 version 1.7.7
InfluxDB shell version: 1.7.7
>  create database telegraf
>  show databases
name: databases
name
----
_internal
telegraf
>  create user "admin" with password 'admin' with all privileges
>  create user "telegraf" with password 'telegraf'
>  show users
user     admin
----     -----
telegraf false
admin    true
>  exit
```

如果docker里没有vim和ps命令，执行如下命令安装：
```shell script
apt-get update && apt-get install -y procps && apt-get install -y vim
```

