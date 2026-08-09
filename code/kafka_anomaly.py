import json
import os
from datetime import datetime
import statistics

import requests
from confluent_kafka import Consumer, Producer
from dotenv import load_dotenv

load_dotenv()
SLACK_URL = os.getenv('SLACK_URL')

MAX_BUFFER_SIZE = 50
VOLUME_THRESHOLD = 3.5
PRICE_THRESHOLD = 2.5

CRYPTO = ['KRW-BTC', ]
PRICE_BUFFER = {i: None for i in CRYPTO}
VOLUME_BUFFER = {i: [] for i in CRYPTO}

producer = Producer(
    {
        'bootstrap.servers': 'localhost:19092'
    }
)

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'crypto_alert',
        'auto.offset.reset': 'earliest'
    }
)

def send_slack_message(code, price, volume, alert_type, rate, timestamp):
    if alert_type == 'price':
        msg = '체결가'
        emoji = ':whale2:'
    else:
        msg = '거래량'
        emoji = ':rotating_light:'

    params = {
        'text': f'''*{code} 종목 {msg} 이상 감지*
        - :calendar: 거래 일시: {datetime.fromtimestamp(timestamp)}
        - :dollar: 체결가: {price}
        - :receipt: 거래량: {volume}
        - :bar_chart: {msg} 변화율: {rate}''',
        'username': 'Alert Bot',
        'icon_emoji': emoji
    }

    requests.post(url = SLACK_URL, json = params)

def modified_z_score(volume_buffer, curr_volume):
    median = statistics.median(volume_buffer)
    mad = statistics.median([abs(i - median) for i in volume_buffer])

    if mad == 0:
        modified_z = 0
    else:
        modified_z = 0.6745 * (curr_volume - median) / mad

    return modified_z

def main():
    topic = 'anomaly_upbit_tickers'
    consumer.subscribe([topic])

    while True:
        try:
            msg = consumer.poll(0)

            if not msg:
                continue

            if msg.error():
                print(f'Kafka Error: {msg.error()}')
                continue

            raw_msg = msg.value().decode()
            parsed_msg = json.loads(raw_msg)

            code = parsed_msg['code']
            curr_price = parsed_msg['trade_price']
            curr_volume = parsed_msg['trade_volume']

            prev_price = PRICE_BUFFER[code]
            buffer = VOLUME_BUFFER[code]

            # 거래량 이상 감지
            if len(buffer) >= MAX_BUFFER_SIZE:
                modified_z = modified_z_score(buffer, curr_volume)
                if abs(modified_z) >= VOLUME_THRESHOLD:
                    producer.produce(
                        topic = 'kafka_anomaly',
                        key = code.encode(),
                        value = json.dumps({
                            'code': code,
                            'trade_price': curr_price,
                            'trade_volume': curr_volume,
                            'alert_type': 'volume',
                            'rate': modified_z,
                            'raw_timestamp': parsed_msg['raw_timestamp'],
                            'timestamp': parsed_msg['timestamp'],
                        }).encode()
                    )
                    producer.poll(0)

                    send_slack_message(
                        code,
                        curr_price,
                        curr_volume,
                        'volume',
                        modified_z,
                        parsed_msg['timestamp']
                    )

            buffer.append(curr_volume)
            VOLUME_BUFFER[code] = buffer[-MAX_BUFFER_SIZE:]

            # 체결가 이상 감지
            if prev_price:
                price_change_rate = ((curr_price - prev_price) / prev_price) * 100
                if abs(price_change_rate) >= PRICE_THRESHOLD:
                    producer.produce(
                        topic = 'kafka_anomaly',
                        key = code.encode(),
                        value = json.dumps({
                            'code': code,
                            'trade_price': curr_price,
                            'trade_volume': curr_volume,
                            'alert_type': 'price',
                            'rate': price_change_rate,
                            'raw_timestamp': parsed_msg['raw_timestamp'],
                            'timestamp': parsed_msg['timestamp'],
                        }).encode()
                    )
                    producer.poll(0)

                    send_slack_message(
                        code,
                        curr_price,
                        curr_volume,
                        'price',
                        price_change_rate,
                        parsed_msg['timestamp']
                    )

            PRICE_BUFFER[code] = curr_price
        
        except Exception as e:
            print(f'Unexpected Error: {e}')
            print(msg.value())

        finally:
            producer.flush()

if __name__ == '__main__':
    try:
        print('Kafka Anomaly Start')
        main()

    except KeyboardInterrupt:
        print('Kafka Anomaly inturrupted')
