# tick-stack-chronograf

1. 部署
```shell script
docker-compose up
```

2. 然后访问本地 chronograf，地址 http://localhost:8888
在 chronograf 中设置 influxdb 和 kapacitor 连接字符串，连接字符串是它们的完全本地IP，例如：
- http://a.b.c.d:8086 for influxdb
- http://a.b.c.d:9092 for kapacitor

See https://www.youtube.com/watch?v=dk7ZdcNsrKE for a demo video

See https://vimeo.com/191737015 for a talk about this stack by Influx 
