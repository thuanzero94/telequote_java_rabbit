# -*- coding: utf-8 -*-
# pylint: disable=C0111,C0103,R0205

import json
import logging
import pika
from pika.exchange_type import ExchangeType

print('pika version: %s' % pika.__version__)

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='192.168.1.99', credentials=pika.credentials.PlainCredentials(username='telequoteFeed',
                                                                                               password='Chunming0684')))

main_channel = connection.channel()


queue = main_channel.queue_declare('test_gcoinx_feed', exclusive=True).method.queue
main_channel.queue_bind(exchange='amq.topic', queue=queue, routing_key='example.changed')


def hello():
    print('Hello world')


connection.call_later(5, hello)

msg = 0
def callback(_ch, _method, _properties, body):
    global msg
    msg += 1
    print(f'[Message #{msg}] {body.decode()}')


logging.basicConfig(level=logging.INFO)

# Note: consuming with automatic acknowledgements has its risks
#       and used here for simplicity.
#       See https://www.rabbitmq.com/confirms.html.
main_channel.basic_consume(queue, callback, auto_ack=True)

try:
    main_channel.start_consuming()
finally:
    connection.close()
