from confluent_kafka import Consumer
import time
import json
from datetime import datetime
import uuid

def get_consumer():
    config = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'crypto_raw',
        'auto.offset.reset': 'earliest'
    }

    return Consumer(config)

def save(records):
    now = datetime.now().strftime('%Y%m%d_%H%M%S')

    with open(f'./raw_data/{now}_{uuid.uuid4().hex}.json', 'w', encoding = 'utf-8') as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii = False) + '\n')

def main():
    consumer = get_consumer()

    topic = 'raw_upbit_tickers'
    consumer.subscribe([topic])

    buffer = []
    last_save_time = time.time()

    try:
        while True:
            msg = consumer.poll(0)

            if msg == None:
                if len(buffer) > 0 and (time.time() - last_save_time) > 60:
                    save(buffer)
                    buffer = []
                    last_save_time = time.time()
                continue

            if msg.error():
                print(f'Kafka Error: {msg.error()}')
                continue
                
            try:
                raw_msg = msg.value().decode()
                parsed_msg = json.loads(raw_msg)
                buffer.append(parsed_msg)

            except Exception as e:
                print(f'Parsing Error: {e}')
                print(msg.value())

            if len(buffer) > 0 and (time.time() - last_save_time) > 60:
                save(buffer)
                buffer = []
                last_save_time = time.time()

    except Exception as e:
        print(f'Unexpected Error: {e}')

    finally:
        consumer.close()
        if len(buffer) > 0:
            save(buffer)

if __name__ == '__main__':
    try:
        print('Consumer Started')
        main()

    except KeyboardInterrupt:
        print('Consumer interrupted. Save remaining messages')
