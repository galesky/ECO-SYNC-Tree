FROM openjdk:8-alpine
LABEL maintainer="Pedro Ákos Costa <pah.costa@campus.fct.unl.pt>"

RUN apk add --no-cache \
                bind-tools \
                iproute2 \
                nload

WORKDIR code
ADD docker/start.sh ./start.sh
ADD target/PlumtreeOpLogs.jar ./PlumtreeOpLogs.jar
ADD config.properties ./config.properties
ADD log4j2.xml ./log4j2.xml

RUN mkdir config

ADD docker/latencies50.txt config/latencies50.txt
ADD docker/latencies100.txt config/latencies100.txt
ADD docker/latencies150.txt config/latencies150.txt
ADD docker/latencies200.txt config/latencies200.txt
ADD docker/ips200.txt config/ips.txt
ADD docker/setupTc.sh .

EXPOSE 10000/tcp

ENTRYPOINT ["./setupTc.sh"]
#CMD ["/bin/sh"]
#ADD tools/* ./