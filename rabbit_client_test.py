import asyncio
import datetime
import functools
import json
import logging
import os
import pathlib
import re
import socket
import threading
import time

import shutil
import pika
import websockets
from queue import Queue
from tendo import singleton

import yaml
from pika.exchange_type import ExchangeType
from time import strftime
from logging.handlers import RotatingFileHandler

working_dir = os.path.dirname(os.path.realpath(__file__))
config_data = None

# When this file running will lock file .tmp_cryptocomapre_feed_lockfile for tool auto_unlock (running in task scheduler)
try:
    lock_file = singleton.SingleInstance(lockfile=os.path.join(working_dir, ".tmp_telequote_java_to_rabbit_lockfile.lock"))
except singleton.SingleInstanceException:
    print("Another telequote_java_to_rabbit.py is running, Exit...")
    exit(-1)

with open(os.path.join(working_dir, 'config.yaml'), 'r') as f:
    config_data = yaml.load(f, Loader=yaml.FullLoader)

LOG_FORMAT = '%(levelname)-8s|%(asctime)s|%(name)-10s|%(funcName)-30s|%(lineno)-5d: %(message)s'
log_dir = os.path.join(working_dir, 'log')
g_log_level = int(config_data.get('log_level', 20))
g_log_file_num = int(config_data.get('log_file_number_limit', 5))
g_log_file_size = int(config_data.get('log_file_size_limit', 30))
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

def create_logger(name, filename, mode, level):
    dir_name = os.path.dirname(filename)
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)
    tmp_logger = logging.getLogger(name)
    handler = RotatingFileHandler(filename=filename, mode=mode,
                                  maxBytes=g_log_file_size*1024*1024, backupCount=g_log_file_num)
    formatter = logging.Formatter(LOG_FORMAT, datefmt='%d-%b-%y %H:%M:%S')
    handler.setFormatter(formatter)
    tmp_logger.addHandler(handler)
    tmp_logger.setLevel(level)
    return tmp_logger

LOGGER = create_logger(__name__, os.path.join(log_dir, f"raw_{strftime('%Y_%m_%d')}.txt"), "a+", g_log_level)


# Cryptocomapre API key
crypto_host_url = config_data['crypto_api'].get('host_url', 'wss://streamer.cryptocompare.com/v2')
api_key = config_data['crypto_api'].get('key', 'None')
crypto_subscriptions = config_data['crypto_api'].get('subscriptions', ["2~Coinbase~BTC~USD", "2~Coinbase~ETH~USD"])
# Data queue
data_queue = Queue()


def get_connection_info(data):
    """
    Args:
      data: a dict with connection info.

    Returns:
        all login info.
    """
    if 'port' in data and not data['port'] is None:
        port = int(data['port'])
    else:
        port = 5672
    if 'server' in data and not data['server'] is None:
        server = str(data['server'])
    else:
        server = 'localhost'
    if 'user' in data and not data['user'] is None:
        user = str(data['user'])
    else:
        user = 'guest'
    if 'password' in data and not data['password'] is None:
        password = str(data['password'])
    else:
        password = 'guest'

    return server, user, password, port


class ExamplePublisher(object):
    """This is an example publisher that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses delivery confirmations and illustrates one way to keep track of
    messages that have been sent and if they've been confirmed by RabbitMQ.

    """
    EXCHANGE = config_data.get('amqp_publisher_info', 'amp.topic').get('exchange', 'amq.topic')
    EXCHANGE_TYPE = config_data.get('amqp_publisher_info', ExchangeType.topic).get('exchange_type', ExchangeType.topic)
    PUBLISH_INTERVAL = config_data.get('publisher_option', 0.01).get('time', 0.01)
    DURABLE = config_data.get('amqp_publisher_info', True).get('durable', True)
    # QUEUE = 'text'
    ROUTING_KEY = config_data.get('amqp_publisher_info', 'example.changed').get('routing_key', 'example.changed')

    def __init__(self, connection_data):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param str amqp_url: The URL for connecting to RabbitMQ

        """
        self._connection = None
        self._channel = None
        self._closing = False

        self._deliveries = None
        self._acked = None
        self._nacked = None
        self._message_number = None

        self._stopping = False
        if connection_data:
            self._host, self._user, self._passwd, self._port = get_connection_info(connection_data)


    def connect(self):
        """This method connects to RabbitMQ, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika.

        :rtype: pika.SelectConnection

        """
        params1 = pika.URLParameters("amqps://lpjgqzqp:Vx3YddbEMROreAu2IRi6XGXcHh1xqIm6@woodpecker.rmq.cloudamqp.com/lpjgqzqp")
        params2 = pika.URLParameters("rabbit://admin:Chunming0684@127.0.0.1:5672/")
        # params = pika.ConnectionParameters(host=self._host, port=self._port,
        #                                    credentials=pika.credentials.PlainCredentials(
        #                                        username=self._user,
        #                                        password=self._passwd),
        #                                    heartbeat=60,
        #                                    connection_attempts=3)
        # LOGGER.info(f'Connecting to {params}')
        return pika.SelectConnection(params2,
                                     on_open_callback=self.on_connection_open,
                                     on_open_error_callback=self.on_connection_open_error,
                                     on_close_callback=self.on_connection_closed)

    def on_connection_open(self, _unused_connection):
        """This method is called by pika once the connection to RabbitMQ has
        been established. It passes the handle to the connection object in
        case we need it, but in this case, we'll just mark it unused.

        :param pika.SelectConnection _unused_connection: The connection

        """
        LOGGER.info('Connection opened')
        self.open_channel()

    def on_connection_open_error(self, _unused_connection, err):
        """This method is called by pika if the connection to RabbitMQ
        can't be established.

        :param pika.SelectConnection _unused_connection: The connection
        :param Exception err: The error

        """
        LOGGER.error('Connection open failed, reopening in 5 seconds: %s', err)
        self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def on_connection_closed(self, _unused_connection, reason):
        """This method is invoked by pika when the connection to RabbitMQ is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        RabbitMQ if it disconnects.

        :param pika.connection.Connection connection: The closed connection obj
        :param Exception reason: exception representing reason for loss of
            connection.

        """
        self._channel = None
        if self._stopping:
            self._connection.ioloop.stop()
        else:
            LOGGER.warning('Connection closed, reopening in 5 seconds: %s', reason)
            self._connection.ioloop.call_later(5, self._connection.ioloop.stop)

    def open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        LOGGER.info('Creating a new channel')
        if not self._channel:
            self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        LOGGER.info('Channel opened')
        self._channel = channel
        # self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        LOGGER.debug('Adding channel close callback')
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed

        """
        LOGGER.warning('Channel %i was closed: %s', channel, reason)
        self._channel = None
        if not self._stopping:
            self._connection.close()

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.

        :param str|unicode exchange_name: The name of the exchange to declare

        """
        LOGGER.debug('Declaring exchange %s', exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(
            self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=self.EXCHANGE_TYPE,
            durable=self.DURABLE,
            callback=cb)

    def on_exchange_declareok(self, _unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.

        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        :param str|unicode userdata: Extra user data (exchange name)

        """
        LOGGER.debug('Exchange declared: %s', userdata)
        self.start_publishing()
        # self.setup_queue(self.QUEUE)

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        LOGGER.info('Start Publishing: Issuing consumer related RPC commands')
        self.enable_delivery_confirmations()
        self.schedule_next_message()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        LOGGER.debug('Issuing Confirm.Select RPC command')
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.

        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

        """
        confirmation_type = method_frame.method.NAME.split('.')[1].lower()
        LOGGER.debug('Received %s for delivery tag: %i', confirmation_type,
                    method_frame.method.delivery_tag)
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.debug(
            'Published %i messages, %i have yet to be confirmed, '
            '%i were acked and %i were nacked', self._message_number,
            len(self._deliveries), self._acked, self._nacked)

    def schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        LOGGER.debug('Scheduling next message for %0.1f seconds', self.PUBLISH_INTERVAL)
        self._connection.ioloop.call_later(self.PUBLISH_INTERVAL, self.publish_message)

    def publish_message(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._channel is None or not self._channel.is_open:
            return
        hdrs = {u'a': u' b', u'c': u'd', u'e': u'f'}
        properties = pika.BasicProperties(
            app_id='example-publisher',
            content_type='application/json',
            headers=hdrs)
        message = data_queue.get()
        q_size = data_queue.qsize()
        LOGGER.debug(f'queue size: {q_size}')
        if q_size > 100:
            LOGGER.warning(f'[WARNING] Queue seem stuck: {q_size}')
        data = json.dumps(message, ensure_ascii=False)
        self._channel.basic_publish(self.EXCHANGE, self.ROUTING_KEY,
                                    data,
                                    properties)
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info('[Published message # %i] %s', self._message_number, message)
        self.schedule_next_message()

    def run(self):
        """Run the example code by connecting and then starting the IOLoop.

        """
        while not self._stopping:
            self._connection = None
            self._deliveries = []
            self._acked = 0
            self._nacked = 0
            self._message_number = 0

            try:
                self._connection = self.connect()
                self._connection.ioloop.start()
            except KeyboardInterrupt:
                self.stop()
                if (self._connection is not None and
                        not self._connection.is_closed):
                    # Finish closing
                    self._connection.ioloop.start()

        LOGGER.info('Stopped')

    def stop(self):
        """Stop the example by closing the channel and connection. We
        set a flag here so that we stop scheduling new messages to be
        published. The IOLoop is started because this method is
        invoked by the Try/Catch below when KeyboardInterrupt is caught.
        Starting the IOLoop again will allow the publisher to cleanly
        disconnect from RabbitMQ.

        """
        LOGGER.info('Stopping')
        self._stopping = True
        self.close_channel()
        self.close_connection()

    def close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._channel is not None:
            LOGGER.info('Closing the channel')
            self._channel.close()

    def close_connection(self):
        """This method closes the connection to RabbitMQ."""
        if self._connection is not None:
            LOGGER.info('Closing connection')
            self._connection.close()

class CleanLogUtility:
    def __init__(self, saved_dir, date_format, keep_days=7, cleaning_cycle=60*60, logger=None):
        self._log_dir = saved_dir
        self._date_format = date_format
        self._keep_days = keep_days
        self._cleaning_cycle = cleaning_cycle
        self._logger = logger if not logger is None else logging
        self._stop = False
        self._is_cleaning = False
        self._init_date = datetime.datetime.today().date()

    def IsDateString(self, time_str, d_format='%Y-%m-%d'):
        """ Check if date string
        Args:
          time_str: time string.
          d_format: format of date time
        Return:
          True: time_string is in the date format
          False: time_string is not in the date format
        """

        try:
            datetime.datetime.strptime(time_str, d_format)
            valid_date = True
        except Exception as exp:
            valid_date = False
        return valid_date

    def CleanupSavedFolders(self, saved_dir, date_format='%Y-%m-%d', regex_filter=None, keep_days=7):
        """cleanup old test log under the saved log directory.

        Args:
          :param regex_filter: filter of date in name string
          :param saved_dir: saved log directory.
          :param keep_days: Max Days Keep the log
          :param date_format: format of date
        """
        keep_log_date = datetime.datetime.today().date() - datetime.timedelta(days=keep_days)
        folders = os.listdir(saved_dir)
        for sub_item in folders:
            sub_item_path = os.path.join(saved_dir, sub_item)
            if os.path.isdir(sub_item_path):  # for delete folder (support recursive)
                if self.IsDateString(sub_item, d_format=date_format):
                    sub_item_date = datetime.datetime.strptime(sub_item, date_format).date()
                    if sub_item_date < keep_log_date:
                        try:
                            shutil.rmtree(sub_item_path)
                            self._logger.debug('Removed :{0}'.format(sub_item_path))
                        except OSError as e:
                            self._logger.debug('Remove {0} failed, Err:{1}'.format(sub_item_path, str(e)))
                            pass
                else:
                    self.CleanupSavedFolders(sub_item_path, date_format=date_format, keep_days=keep_days)
            elif os.path.isfile(sub_item_path):  # for delete File (a special flag for delete raw_log)
                try:
                    # Correct sub_item format: raw_2021_03_04.txt, raw_2021_03_04.txt.1
                    str_date = re.search(regex_filter, sub_item)[0]
                except Exception as e:
                    self._logger.debug(f'Incorrect format - Remove {sub_item}')
                    continue
                if self.IsDateString(str_date, d_format=date_format):
                    sub_item_date = datetime.datetime.strptime(str_date, '%Y_%m_%d').date()
                    if sub_item_date < keep_log_date and sub_item_date != self._init_date:
                        try:
                            os.unlink(sub_item_path)
                            self._logger.debug('Removed :{0}'.format(sub_item_path))
                        except OSError as e:
                            self._logger.debug('Remove {0} failed, Err:{1}'.format(sub_item_path, str(e)))
                            pass

    def start_cleaning(self):
        self._stop = False
        start_time = time.time()
        self._logger.debug(f'=== Init Clean Worker: dir: "{self._log_dir}", Time: {self._cleaning_cycle}s ===')
        while not self._stop:
            if time.time() - start_time > self._cleaning_cycle:
                self._is_cleaning = True
                try:
                    self._logger.debug('=== Start Cleaning ===')
                    # Clean log
                    self.CleanupSavedFolders(self._log_dir, date_format='%Y_%m_%d', regex_filter="([0-9]{4}\_[0-9]{2}\_[0-9]{2})", keep_days=self._keep_days)
                except Exception as e:
                    self._logger.debug('*** Cleaning task failed! Error message:{0} ***'.format(str(e)))
                self._logger.debug('=== Cleaning Done ===')
                self._is_cleaning = False
                start_time = time.time()
            time.sleep(2)

    def stop_cleaning(self):
        self._logger.debug('STOP Cleaning, Waiting Cleaning Done...')
        while self._is_cleaning:
            time.sleep(1)
        self._stop = True
        self._logger.debug('STOPPED')

# Pull data by Socket from Java Telequote
def pull_from_socket_java():
    try:
        read_timeout = 60
        # Create a socket object
        s = socket.socket()

        # Define the port on which you want to connect
        host = config_data.get('server_relay', {}).get('host', '127.0.0.1')
        port = config_data.get('server_relay', {}).get('port', 42222)
        LOGGER.info('------- Start Get data from Java relay (telequote) --------')

        # connect to the server on local computer
        status = s.connect((host, port))
        LOGGER.info(status)
        start_time = time.time()
        count = 0
        while True:
            if time.time() - start_time > read_timeout:
                raise Exception('Read from Server Timeout. Close Client!')
                break
            # receive data from the server and decoding to get the string.
            raw = s.recv(1024)
            d_decode = raw.decode()
            if len(d_decode) > 0:
                count += 1
                start_time = time.time()
                # print(raw)
                # print(f'{count}: {d_decode}', end='')
                LOGGER.debug(f'{count}: {d_decode}')
                data_queue.put_nowait(d_decode)
                # time.sleep(1)
                s.send(b'OK\n')
            # close the connection
    except Exception as e:
        LOGGER.error(str(e))
        pass
    s.close()

def launch_program():
    global backend_threads
    # Clean Thread
    clean_thread = backend_threads.get('clean_thread')
    if not clean_thread or not clean_thread.isAlive():
        LOGGER.debug("Init Clean Worker...")
        clean_worker = CleanLogUtility(log_dir, date_format='%Y_%m_%d', keep_days=config_data.get('log_keep_days', 14),
                                       cleaning_cycle=int(config_data.get('log_cleaning_time', 60))*60, logger=LOGGER)
        clean_thread = threading.Thread(target=clean_worker.start_cleaning, name='Cleaner')
        clean_thread.start()
        backend_threads['clean_thread'] = clean_thread

    # Publish Thread
    publish_thread = backend_threads.get('publish_thread')
    if not publish_thread or not publish_thread.isAlive():
        LOGGER.info('publish thread Shutoff, wait 10 seconds before connect again...')
        time.sleep(5)
        # Connect to localhost:5672 as guest with the password guest and virtual host "/" (%2F)
        # url: 'amqp://guest:guest@localhost:5672/%2F?connection_attempts=3&heartbeat=3600'
        default_connection = {'server': 'localhost', 'port': 5672, 'user': 'datafeed', 'password': 'Datafeed#18'}
        example_publisher = ExamplePublisher(config_data.get('rabbit_server', default_connection))
        publish_thread = threading.Thread(target=example_publisher.run, name='publish')
        # publish_thread.daemon = True
        publish_thread.start()
        backend_threads['publish_thread'] = publish_thread
    # Pull Thread
    # pull_thread = backend_threads.get('pull_thread')
    # if not pull_thread or not pull_thread.isAlive():
    #     LOGGER.info('Pull from Java thread Shutoff, wait 10 seconds before connect again...')
    #     time.sleep(5)
    #     pull_thread = threading.Thread(target=pull_from_socket_java, name='pull')
    #     # pull_thread.daemon = True
    #     pull_thread.start()
    #     backend_threads['pull_thread'] = pull_thread
    # asyncio.get_event_loop().run_until_complete(cryptocompare())

def filter_crypto_stream_data(data):
    return True

async def cryptocompare():
    recv_count = 0
    url = f"{crypto_host_url}?api_key={api_key}"
    sub_msg = json.dumps({
        "action": "SubAdd",
        "subs": crypto_subscriptions,
    })
    async with websockets.connect(url) as ws:
        await ws.send(sub_msg)  # Subscribe Crypto Channels
        while True:
            try:
                data = await ws.recv()
            except websockets.ConnectionClosed:
                if not ws.open:  # Handle Reconnect
                    try:
                        LOGGER.info('[cryptocompare] Websocket NOT connected. Trying to reconnect.')
                        ws = await websockets.connect(url)
                        await ws.send(sub_msg)
                        LOGGER.info('[cryptocompare] Websocket Connected!')
                        await asyncio.sleep(3)
                    except Exception as expt:
                        LOGGER.info(f'[cryptocompare error] {expt}')
                        await asyncio.sleep(5)
                continue
                # break
            try:
                data = json.loads(data)
                if data['TYPE'] == '2' or data['TYPE'] == '5':
                    data_queue.put_nowait(data)
                    recv_count += 1
                    LOGGER.info(f'[cryptocompare-{len(data)}][Data Received #{recv_count}]')
                else:
                    LOGGER.info(f'[cryptocompare{-len(data)}][Data Received]{json.dumps(data)}')
                LOGGER.debug(f'[cryptocompare-{len(data)}][Data Received]{json.dumps(data)}')
            except Exception as expt:
                if expt is ValueError:
                    LOGGER.error(f'[cryptocompare] data is not json: {data}')
                else:
                    LOGGER.error(f'[cryptocompare] {expt}')


if __name__ == '__main__':
    # pull_from_socket_java()
    # exit()
    logging.basicConfig(level=logging.ERROR, format=LOG_FORMAT)
    backend_threads = {}
    launch_program()
    while True:
        launch_program()
        time.sleep(5)
