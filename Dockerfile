FROM apache/spark:4.1.1-scala2.13-java17-python3-ubuntu

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pandas pyarrow protobuf

RUN mkdir -p "${SPARK_HOME}/conf" && \
    mkdir -p /tmp/.ivy2/jars /tmp/.ivy2/cache && \
    chmod -R 777 /tmp/.ivy2 && \
    echo "spark.jars.ivy /tmp/.ivy2" >> "${SPARK_HOME}/conf/spark-defaults.conf"

USER spark