from confluent_kafka import Consumer
from datetime import datetime
import uuid
import json
import time

consumer = Consumer(
    {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'crypto_raw',
        'auto.offset.reset': 'earliest'
    }
)

def save(records):
    now = datetime.now().strftime('%Y%m%d_%H%M%S')

    # 파일 경로가 해당 스크립트를 실행한 지점에서의 하위 폴더(raw_data)이므로
    # 원하는 경로에 저장하기 위해서는 프로젝트 루트 폴더에서 스크립트를 실행해야 함
    with open(f'./raw_data/{now}_{uuid.uuid4().hex}.json', 'w', encoding = 'utf-8') as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii = False) + '\n')

def main():
    topic = 'raw_upbit_tickers'
    consumer.subscribe([topic])

    buffer = []
    last_save_time = time.time()

    try:
        while True:
            # 1분 간격으로 파일을 저장
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

            data = msg.value().decode()
            parsed_data = json.loads(data)
            buffer.append(parsed_data)

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
        print('Consumer interrupted. Save remaining messages...')
