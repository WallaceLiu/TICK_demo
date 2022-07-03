FROM golang:1.13 as BUILD
RUN go get github.com/influxdata/kapacitor/cmd/kapacitord

FROM ubuntu:18.04
VOLUME /var/lib/influxdb
EXPOSE 8086
COPY --from=BUILD /go/bin/ /bin/
CMD ["kapacitord"]