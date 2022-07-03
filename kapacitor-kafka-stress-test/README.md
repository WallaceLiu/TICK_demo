This is an application that can be used to stress test the [Kafka alert](https://docs.influxdata.com/kapacitor/v1.5/event_handlers/kafka/)
handler of [Kapacitor](https://www.influxdata.com/time-series-platform/kapacitor/). It provides
a REST API for initiating scenarios to run and a Kafka consumer to help verify the resulting
alerts were propagated through Kafka.

Each scenario is composed of 
- metrics generation interval
- metrics to send, consisting of
  - measurement name
  - fields
  - values to iterate over
- kapacitor task definition
  - measurement
  - lambda expression for critical alerts
  
# Running

## Docker support services

This project comes with a Docker compose file that defines a Kafka and Kapacitor service to run

Start the support services using
```
docker-compose up -d
```

## Start the Spring Boot application

There are several ways to run a Spring Boot application locally. One way is using Maven:

```
mvn spring-boot:run
```

# Operations

The [testing.http](testing.http) file contains examples of the REST operations that can
be performed to start/stop scenarios and query the consumer counts per scenario+measurement.