# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import json
import logging
import pika
from pika.exchange_type import ExchangeType

print('pika version: %s' % pika.__version__)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='data.tradeprofx.com', credentials=pika.credentials.PlainCredentials(username='telequoteFeed',
                                                                                               password='Chunming0684')))

main_channel = connection.channel()


queue = main_channel.queue_declare('test_gcoinx_feed', exclusive=True).method.queue
main_channel.queue_bind(exchange='amq.topic', queue=queue, routing_key='example.changed')


def hello():
    print('Hello world')


connection.call_later(5, hello)

msg = 0
counter_list = []
def callback(_ch, _method, _properties, body):
    global msg
    msg += 1
    a= body.decode()
    res = json.loads(a).replace('\r\n', '')
    res = json.loads(res)
    # print(res["symbol"])
    if res.get('symbol', None) is None:
        print(f"fail: {res}")
    elif f"{res['symbol']}" not in counter_list:
        counter_list.append(f"{res['symbol']}")
        print(f'{len(counter_list)}: {counter_list}')
    # print(f'[Message #{msg}] {res}')


logging.basicConfig(level=logging.INFO)

# Note: consuming with automatic acknowledgements has its risks
#       and used here for simplicity.
#       See https://www.rabbitmq.com/confirms.html.
main_channel.basic_consume(queue, callback, auto_ack=True)

try:
    main_channel.start_consuming()
finally:
    connection.close()
