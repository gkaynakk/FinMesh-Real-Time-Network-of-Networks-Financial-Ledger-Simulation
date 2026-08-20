FROM flink:1.20.1-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=3.3.0-1.20

RUN wget -P /opt/flink/lib \
    https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar