from pyspark import SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, TimestampType
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
import pyspark.sql.functions as f

from typing import Iterator

N = 50
PRICE_THRESHOLD = 2.5
VOLUME_THRESHOLD = 10.0

# for Test
#N = 10
#PRICE_THRESHOLD = 0.01
#VOLUME_THRESHOLD = 0.01

class AnomalyDetectorProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle

        price_schema = StructType([StructField('price', DoubleType(), True)])
        self.price = handle.getValueState('price', price_schema)

        trade_schema = StructType([StructField('volume', DoubleType(), True)])
        self.trade = handle.getListState('trade', trade_schema)

    def handleInputRows(self, key, rows, timerValues) -> Iterator[Row]:
        output = []

        for row in rows:
            curr_price = row.trade_price
            curr_volume = row.trade_volume

            # 거래량 이상 감지
            if self.trade.exists():
                trade_buffer = [i[0] for i in self.trade.get()]
            else:
                trade_buffer = []

            if len(trade_buffer) >= N:
                volume_change_rate = curr_volume / (sum(trade_buffer) / N)
                if volume_change_rate >= VOLUME_THRESHOLD:
                    output.append(
                        Row(code = key[0],
                            curr_trade_price = curr_price,
                            curr_trade_volume = curr_volume,
                            alert_type = 'volume',
                            ratio = volume_change_rate,
                            raw_timestamp = row.raw_timestamp,
                            timestamp = row.timestamp)
                    )
            
            trade_buffer.append(curr_volume)
            trade_buffer = trade_buffer[-N:]
            self.trade.put([(i, ) for i in trade_buffer])

            # 체결가 이상 감지
            if self.price.exists():
                prev_price = self.price.get()[0]
                price_change_rate = ((curr_price - prev_price) / prev_price) * 100
                if abs(price_change_rate) >= PRICE_THRESHOLD:
                    output.append(
                        Row(code = key[0],
                            curr_trade_price = curr_price,
                            curr_trade_volume = curr_volume,
                            alert_type = 'price',
                            ratio = price_change_rate,
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
    #conf.set('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1')
    
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

    kafka_input_schema = StructType([
        StructField('code', StringType(), True),
        StructField('trade_price', DoubleType(), True),
        StructField('trade_volume', DoubleType(), True),
        StructField('raw_timestamp', LongType(), True),
        StructField('timestamp', TimestampType(), True)
    ])

    output_schema = StructType([
        StructField('code', StringType(), True),
        StructField('curr_trade_price', DoubleType(), True),
        StructField('curr_trade_volume', DoubleType(), True),
        StructField('alert_type', StringType(), True),
        StructField('ratio', DoubleType(), True),
        StructField('raw_timestamp', LongType(), True),
        StructField('timestamp', TimestampType(), True),
    ])

    query = input_df.select(f.from_json(f.col('value').cast('string'), schema = kafka_input_schema).alias('values'))\
                .select(f.col('values.*'))\
                .groupBy('code')\
                .transformWithState(
                    statefulProcessor = AnomalyDetectorProcessor(),
                    outputStructType = output_schema,
                    outputMode = 'update',
                    timeMode = 'None'
                )\
                .writeStream\
                .format('console')\
                .option('checkpointLocation', './spark_checkpoint')\
                .start()
    
    query.awaitTermination()
