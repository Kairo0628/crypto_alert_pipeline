from confluent_kafka import Producer
import websockets
import uuid
import json
import asyncio

class MessageCounter:
    # 전송 메시지 카운터 클래스
    # 메시지 카운터와 on_delivery 함수를 여기서 정의
    # 메시지가 정상적으로 전송된 경우 카운터 증가

    # 로그가 대량으로 출력되는 것을 방지하기 위해
    # 일반적인 상황에서는 출력하지 않고
    # 1,000개의 메시지 단위로 전송이 완료된 경우 출력
    def __init__(self):
        self.msg_count = 0

    def delivery_callback(self, err, msg):
        if err:
            print(f'{msg.topic()} Message delivery Failed: {err}')
            print()
            return
        
        self.msg_count += 1
        if self.msg_count % 1_000 == 0:
            print(f'Total {self.msg_count:,} {msg.topic()} messages successfully delivered !')
            print(f'Recent {msg.topic()} message: {json.loads(msg.value())}')
            print()

def get_producer():
    config = {
        'bootstrap.servers': 'localhost:19092',

        # Kafka Producer 설계

        # Low Latency
        #   - 실시간으로 빠르게 이상 감지를 해야 함

        # Idempotence
        #   - 데이터가 오류로 유실되면 계산에 오류 발생
        #   - 유실된 데이터가 이상 변동 데이터였다면 감지 실패

        # -> 낮은 지연 시간도 중요하지만 데이터가 빠짐 없이 입력되는 것도 중요
        # -> 재시도가 너무 오래걸리면 실시간성이 줄어들기 때문에 상한선 설정
        
        'linger.ms': 5, # 메시지 배치 처리 대기 시간, 5ms
        'acks': -1, # 복제본 저장 결과를 확인할지 여부
        'batch.size': 1024 * 32, # 배치 최대 크기
        'max.in.flight.requests.per.connection': 5, # 한 번에 보낼 수 있는 메시지 개수
        'compression.type': 'lz4', # 메시지 압축 방식. 압축률은 작지만 적은 CPU 사용으로 빠른 처리 가능
        'retries': 5, # 메시지 전송 오류 시 재시도 횟수
        'request.timeout.ms': 1000, # 브로커로부터 전송 완료 응답 대기 시간. 넘으면 재시도
        'retry.backoff.ms': 60, # 메시지 전송 재시도 대기 시간
        'retry.backoff.max.ms': 300, # 최대 메시지 전송 재시도 누적 대기 시간
        'delivery.timeout.ms': 1500, # 메시지 생성부터 전송 완료까지 존재 가능한 시간
    }

    return Producer(config)

async def connect_and_create_topic(producer):
    # Upbit에서 제공하는 가상화폐 실시간 현재가 (Ticker)
    # 받아오는 종목: 비트코인(BTC),
    ws_uri = 'wss://api.upbit.com/websocket/v1'
    ws_msg = [
        {'ticket': str(uuid.uuid1())},
        
        {'type': 'ticker',
         'codes': ['KRW-BTC',
                   ],
         'is_only_realtime': True}
    ]

    raw_counter = MessageCounter()
    anomaly_counter = MessageCounter()

    # 웹소켓 연결 반복
    while True:
        try:
            print('Websocket Connecting...')

            async with websockets.connect(
                uri = ws_uri,
                open_timeout = 10,
                ping_interval = 10,
                ping_timeout = 10,
                close_timeout = 10
            ) as ws:
                await ws.send(json.dumps(ws_msg))

                print('Websocket connect Success')

                while True:
                    stream = await ws.recv()
                    raw_data = json.loads(stream)

                    producer.produce(
                        topic = 'raw_upbit_tickers',
                        key = raw_data['code'].encode(),
                        value = json.dumps(raw_data).encode(),
                        on_delivery = raw_counter.delivery_callback
                    )

                    anomaly_data = {
                        'code': raw_data['code'],
                        'trade_price': raw_data['trade_price'],
                        'signed_change_rate': raw_data['signed_change_rate'],
                        'trade_volume': raw_data['trade_volume'],
                        'timestamp': raw_data['timestamp'] // 1000
                    }

                    producer.produce(
                        topic = 'anomaly_upbit_tickers',
                        key = anomaly_data['code'].encode(),
                        value = json.dumps(anomaly_data).encode(),
                        on_delivery = anomaly_counter.delivery_callback
                    )

                    producer.poll(0)

        except websockets.exceptions.ConnectionClosed:
            print('Websocket connection Closed by Server')

        except Exception as e:
            print(f'Unexpected Error: {e}')

        finally:
            producer.flush()

        print('Reconnecting...')
        await asyncio.sleep(10)

async def main(producer):
    await connect_and_create_topic(producer)

if __name__ == '__main__':
    producer = get_producer()

    try:
        print('Producer Started')
        asyncio.run(main(producer))

    except KeyboardInterrupt:
        print('Producer interrupted. Flush remaining messages...')
