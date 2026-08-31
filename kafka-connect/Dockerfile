FROM apache/kafka:4.2.1

WORKDIR /opt/kafka

COPY *.properties /opt/kafka/config/

RUN mkdir -p /opt/kafka/connect-plugins/jdbc && \
    chmod -R 777 /opt/kafka/connect-plugins

COPY /jdbc/*.jar /opt/kafka/connect-plugins/jdbc/

RUN wget -O /opt/kafka/connect-plugins/jdbc/commons-lang3-3.20.0.jar https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.20.0/commons-lang3-3.20.0.jar
