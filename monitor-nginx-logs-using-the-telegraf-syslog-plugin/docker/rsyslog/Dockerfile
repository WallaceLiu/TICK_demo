FROM alpine:3.11.6
ENV RSYSLOG_VERSION=8.1911.0-r1
RUN apk add --no-cache rsyslog=${RSYSLOG_VERSION} rsyslog-tls=${RSYSLOG_VERSION}
COPY rsyslog.conf /etc/rsyslog.conf
EXPOSE 514/udp
CMD ["rsyslogd", "-n"]
