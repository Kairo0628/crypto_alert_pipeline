from collections.abc import Iterator
import statistics

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType, TimestampType

MAX_BUFFER_SIZE = 50
VOLUME_BASELINE_THRESHOLD = 5
VOLUME_HISTORY_THRESHOLD = 10
PRICE_THRESHOLD = 1.5

def modified_z_score(volume_buffer, curr_volume):
    median = statistics.median(volume_buffer)
    mad = statistics.median([abs(i - median) for i in volume_buffer])

    if mad == 0:
        modified_z = 0
    else:
        modified_z = 0.6745 * (curr_volume - median) / mad

    return modified_z

class AnomalyDetectorProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle

        price_schema = StructType([StructField('price', DoubleType(), True)])
        self.price = handle.getListState('price', price_schema)

        volume_schema = StructType([StructField('volume', DoubleType(), True)])
        self.volume_baseline = handle.getListState('volume_baseline', volume_schema)
        self.volume_history = handle.getListState('volume_history', volume_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        output = []

        for row in rows:
            curr_price = row.trade_price
            curr_volume = row.trade_volume

            # 상태 값 가져오기
            if self.volume_baseline.exists():
                volume_baseline = [i[0] for i in self.volume_baseline.get()]
            else:
                volume_baseline = []

            if self.volume_history.exists():
                volume_history = [i[0] for i in self.volume_history.get()]
            else:
                volume_history = []

            if self.price.exists():
                price_buffer = [i[0] for i in self.price.get()]
            else:
                price_buffer = []

            # 거래량 이상 감지
            # 직전 50개의 거래량 데이터를 모아 이상 감지
            # volume_baseline: 거래량 버퍼1. 이상 감지된 거래량은 추가되지 않음
            # volume_history: 거래량 버퍼2. 전체 거래량 포함
            # volume_baseline의 MAD를 계산하여 5배 높은 경우 이상 거래 후보로 간주
            # volume_history의 평균과 비교하여 최근 거래량보다 10배 이상 차이를 보인다면 이상 거래로 간주
            # 주의: 소수점 단위로 거래가 가능하므로 잦은 이상 감지 가능: 0.01 -> 1 (100배)
            is_volume_alert = False
            if len(volume_baseline) >= MAX_BUFFER_SIZE:
                modified_z = modified_z_score(volume_baseline, curr_volume)

                if abs(modified_z) >= VOLUME_BASELINE_THRESHOLD \
                    and curr_volume >= statistics.mean(volume_history) * VOLUME_HISTORY_THRESHOLD:
                    is_volume_alert = True

                    output.append(
                        Row(code = key[0],
                            trade_price = curr_price,
                            trade_volume = curr_volume,
                            alert_type = 'volume',
                            rate = modified_z,
                            raw_timestamp = row.raw_timestamp,
                            timestamp = row.timestamp)
                    )
            
            if not is_volume_alert:
                volume_baseline.append(curr_volume)
                volume_baseline = volume_baseline[-MAX_BUFFER_SIZE:]
                self.volume_baseline.put([(i, ) for i in volume_baseline])
            volume_history.append(curr_volume)
            volume_history = volume_history[-MAX_BUFFER_SIZE:]
            self.volume_history.put([(i, ) for i in volume_history])

            # 체결가 이상 감지
            # 직전 50개의 거래를 모아 평균을 계산하여 2.5배 이상 높은 경우 이상 거래로 간주
            if len(price_buffer) >= MAX_BUFFER_SIZE:
                mean_price = statistics.mean(price_buffer)
                price_change_rate = (curr_price - mean_price) / mean_price * 100

                if abs(price_change_rate) >= PRICE_THRESHOLD:
                    is_price_alert = True

                    output.append(
                        Row(code = key[0],
                            trade_price = curr_price,
                            trade_volume = curr_volume,
                            alert_type = 'price',
                            rate = price_change_rate,
                            raw_timestamp = row.raw_timestamp,
                            timestamp = row.timestamp)
                    )

            price_buffer.append(curr_price)
            price_buffer = price_buffer[-MAX_BUFFER_SIZE:]
            self.price.put([(i, ) for i in price_buffer])

        yield from iter(output)

if __name__ == '__main__':
    conf = SparkConf()
    conf.set('spark.app.name', 'PySpark Anomaly Detector')
    conf.set('spark.master', 'local[4]')
    conf.set('spark.sql.shuffle.partitions', '4')
    conf.set('spark.sql.streaming.stateStore.providerClass',
             'org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider')
    
    spark = SparkSession.builder\
            .config(conf = conf)\
            .getOrCreate()
    
    spark.sparkContext.setLogLevel('WARN')

    input_df = spark.readStream\
                .format('kafka')\
                .option('kafka.bootstrap.servers', 'broker:9092')\
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
                .option('kafka.bootstrap.servers', 'broker:9092')\
                .option('topic', 'spark_anomaly')\
                .option('checkpointLocation', '/opt/spark/spark_checkpoint')\
                .start()
    
    query.awaitTermination()
