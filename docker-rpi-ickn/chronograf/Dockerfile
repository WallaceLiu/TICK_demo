FROM    resin/rpi-raspbian:stretch

EXPOSE  8888

VOLUME  /var/lib/chronograf

RUN     apt-get update
RUN     apt-get install -y lsof curl net-tools vim sed apt-transport-https

#add influx db repo
RUN     curl -L https://repos.influxdata.com/influxdb.key | sudo apt-key add -
COPY    influxdb.list /etc/apt/sources.list.d/influxdb.list
RUN     apt-get update

RUN     apt-get install -y chronograf

CMD     /usr/bin/chronograf
