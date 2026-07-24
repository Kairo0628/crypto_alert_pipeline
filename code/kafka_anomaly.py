from confluent_kafka import Consumer
from collections import deque
import json
import requests
from datetime import datetime

from dotenv import load_dotenv
import os

N = 50
PRICE_THRESHOLD = 2.5
VOLUME_THRESHOLD = 10.0

# for Test
#N = 10
#PRICE_THRESHOLD = 0.01
#VOLUME_THRESHOLD = 3

load_dotenv()
SLACK_URL = os.getenv('SLACK_URL')

def get_consumer():
    config = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'crypto_alert',
        'auto.offset.reset': 'earliest'
    }

    return Consumer(config)

def main():
    consumer = get_consumer()

    topic = 'anomaly_upbit_tickers'
    consumer.subscribe([topic])

    volume_buffer = {
        'KRW-BTC': deque(maxlen = N)
    }

    prev_price = None

    try:
        while True:
            msg = consumer.poll(0)

            if msg == None:
                continue

            if msg.error():
                print(f'Kafka Error: {msg.error()}')
                continue

            try:
                raw_msg = msg.value().decode()
                parsed_msg = json.loads(raw_msg)

                curr_code = parsed_msg['code']
                curr_buffer = volume_buffer[curr_code]

                curr_volume = parsed_msg['trade_volume']
                curr_price = parsed_msg['trade_price']

                # 거래량 이상 감지
                if len(curr_buffer) >= N:
                    volume_change_rate = curr_volume / (sum(curr_buffer) / N)
                    if volume_change_rate >= VOLUME_THRESHOLD:
                        params = {
                            'text': f'''*거래량 이상 발생*
                            - 발생 일시: {datetime.fromtimestamp(parsed_msg['timestamp'])}
                            - 체결가: {curr_price}
                            - 거래량: {curr_volume}
                            - 거래량 변화율: {volume_change_rate}''',
                            'username': 'Alert Bot',
                            'icon_emoji': ':whale2:'
                        }
                        requests.post(url = SLACK_URL, json = params)
                
                curr_buffer.append(curr_volume)

                # 체결가 이상 감지
                if prev_price is not None:
                    price_change_rate = ((curr_price - prev_price) / prev_price) * 100
                    if abs(price_change_rate) >= PRICE_THRESHOLD:
                        params = {
                            'text': f'''*체결가 이상 발생*
                            - 발생 일시: {datetime.fromtimestamp(parsed_msg['timestamp'])}
                            - 체결가: {curr_price}
                            - 거래량: {curr_volume}
                            - 체결가 변화율: {price_change_rate}''',
                            'username': 'Alert Bot',
                            'icon_emoji': ':rotating_light:'
                        }
                        requests.post(url = SLACK_URL, json = params)

                prev_price = curr_price

            except Exception as e:
                print(f'Parsing Error: {e}')
                print(msg.value())

    except Exception as e:
        print(f'Unexpected Error: {e}')

if __name__ == '__main__':
    try:
        print('Kafka Anomaly Start')
        main()

    except KeyboardInterrupt:
        print('Application inturrupted')
