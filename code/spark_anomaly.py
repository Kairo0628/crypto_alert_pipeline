from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
import pyspark.sql.functions as F
from typing import Iterator

MAX_BUFFER_SIZE = 50
VOLUME_THRESHOLD = 15.0
PRICE_THRESHOLD = 2.5

class AnomalyDetectorProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle

        price_schema = StructType([StructField('price', DoubleType(), True)])
        self.price = handle.getValueState('price', price_schema)

        volume_schema = StructType([StructField('volume', DoubleType(), True)])
        self.volume = handle.getListState('volume', volume_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        output = []

        for row in rows:
            curr_price = row.trade_price
            curr_volume = row.trade_volume

            # 거래량 이상 감지
            # 소수점 단위로 거래가 가능하므로 직전 거래를 기준으로 할 시 잦은 오류 발생 가능: 0.01 -> 1 (100배)
            # 직전 50개의 거래를 모아 평균을 계산하여 10배 높은 경우 이상 거래로 간주
            if self.volume.exists():
                volume_buffer = [i[0] for i in self.volume.get()]
            else:
                volume_buffer = []

            if len(volume_buffer) >= MAX_BUFFER_SIZE:
                volume_change_rate = curr_volume / (sum(volume_buffer) / MAX_BUFFER_SIZE)
                if volume_change_rate >= VOLUME_THRESHOLD:
                    output.append(
                        Row(code = key[0],
                            trade_price = curr_price,
                            trade_volume = curr_volume,
                            alert_type = 'volume',
                            rate = volume_change_rate,
                            raw_timestamp = row.raw_timestamp,
                            timestamp = row.timestamp)
                    )
            
            volume_buffer.append(curr_volume)
            volume_buffer = volume_buffer[-MAX_BUFFER_SIZE:]
            self.volume.put([(i, ) for i in volume_buffer])

            # 체결가 이상 감지
            # 직전 거래가를 기준으로 2.5배 이상 변화했다면 이상 거래로 간주
            if self.price.exists():
                prev_price = self.price.get()[0]

                price_change_rate = (curr_price - prev_price) / prev_price * 100
                if abs(price_change_rate) >= PRICE_THRESHOLD:
                    output.append(
                        Row(code = key[0],
                            trade_price = curr_price,
                            trade_volume = curr_volume,
                            alert_type = 'price',
                            rate = price_change_rate,
                            raw_timestamp = row.raw_timestamp,
                            timestamp = row.timestamp)
                    )

            self.price.update((curr_price, ))

        yield from iter(output)

if __name__ == '__main__':
    conf = SparkConf()
    conf.set('spark.app.name', 'PySpark Anomaly Detector')
    conf.set('spark.master', 'local[4]')
    conf.set('spark.sql.shuffle.partitions', '8')
    conf.set('spark.sql.streaming.stateStore.providerClass',
             'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    
    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    input_df = spark.readStream\
                .format('kafka')\
                .option('kafka.bootstrap.servers', 'localhost:19092')\
                .option('subscribe', 'anomaly_upbit_tickers')\
                .option('startingOffsets', 'earliest')\
                .load()
    
    input_schema = StructType([
        StructField('code', StringType(), True),
        StructField('trade_price', DoubleType(), True),
        StructField('trade_volume', DoubleType(), True),
        StructField('raw_timestamp', LongType(), True),
        StructField('timestamp', TimestampType(), True)
    ])

    output_schema = StructType([
        StructField('code', StringType(), True),
        StructField('trade_price', DoubleType(), True),
        StructField('trade_volume', DoubleType(), True),
        StructField('alert_type', StringType(), True),
        StructField('rate', DoubleType(), True),
        StructField('raw_timestamp', LongType(), True),
        StructField('timestamp', TimestampType(), True),
    ])

    query = input_df.select(F.from_json(F.col('value').cast('string'), schema = input_schema).alias('values'))\
                .select(F.col('values.*'))\
                .groupBy('code')\
                .transformWithState(
                    statefulProcessor = AnomalyDetectorProcessor(),
                    outputStructType = output_schema,
                    outputMode = 'update',
                    timeMode = 'None'
                )\
                .select(F.col('code').cast('string').alias('key'),
                        F.to_json(F.struct('*')).alias('value'))\
                .writeStream\
                .format('kafka')\
                .option('kafka.bootstrap.servers', 'localhost:19092')\
                .option('topic', 'spark_anomaly')\
                .option('checkpointLocation', './spark_checkpoint')\
                .start()
    
    query.awaitTermination()
