from kombu.mixins import ConsumerMixin
from kombu import Connection
import Queue
import time
import logging
import socket
import json
import traceback


class PlopService(ConsumerMixin):
    NAME = 'plop'
    BROKER_URI = "redis://127.0.0.1:6379"
    BROKER_QUEUE = "logstash-results"
    LOGSTASH_URI = "udp://127.0.0.1:5002"
    LOGSTASH_QUEUE = "logstash-ingest"

    def __init__(self, broker_uri=BROKER_URI, broker_queue=BROKER_QUEUE,
                 logstash_uri=LOGSTASH_URI,
                 name=NAME, msg_limit=100):

        self.broker_uri = broker_uri
        self.broker_queue = broker_queue
        self.logstash_uri = logstash_uri

        self.keep_running = False
        self.msg_limit = msg_limit

    def _read_messages(self, uri, queue, callback=None, cnt=1):
        msgs = []
        read_all = False
        if cnt < 1:
            read_all = True

        try:
            logging.debug("Reading the messages")
            with Connection(uri) as conn:
                q = conn.SimpleQueue(queue)
                while cnt > 0 or read_all:
                    cnt += -1
                    try:
                        message = q.get(block=False)
                        if callback is not None:
                            data = callback(message.payload)
                            msgs.append(data)
                            logging.debug("made it here 2")
                            logging.debug(data)
                        message.ack()
                    except Queue.Empty:
                        logging.debug("%s queue is empty" % queue)
                        break
                    except:
                        tb = traceback.format_exc()
                        logging.debug("[XXX] Error: "+tb)
            logging.debug("Successfully read %d messages" % len(msgs))
        except:
            tb = traceback.format_exc()
            logging.debug("[XXX] Error: "+tb)
            logging.debug("Failed to read message")
        return msgs

    def send_logstash(self, etl_data):
        try:
            if self.logstash_uri.find('udp'):
                ip = int(self.logstash_uri.split('://')[1].split(":")[0])
                port = int(self.logstash_uri.split('://')[1].split(":")[1])
                logging.debug("Sending data to UDP %s:%s" % (ip, port))
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.sendto(etl_data, (ip, port))
            if self.logstash_uri.find('tcp'):
                ip = int(self.logstash_uri.split('://')[1].split(":")[0])
                port = int(self.logstash_uri.split('://')[1].split(":")[1])
                logging.debug("Sending data to TCP %s:%s" % (ip, port))
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((ip, port))
                sock.send(etl_data)
        except:
            tb = traceback.format_exc()
            logging.debug("[XXX] Error: "+tb)
            logging.debug("Failed to send etl_data to logstash")

    def process_and_report(self, incoming_msg):
        logging.debug("Processing and report syslog_msg")
        etl_data = incoming_msg
        if isinstance(incoming_msg, str):
            try:
                etl_data = json.loads(incoming_msg)
            except:
                etl_data = {}
                tb = traceback.format_exc()
                logging.debug("[XXX] Error: "+tb)
                raise

        if len(etl_data) > 0:
            self.store_results(etl_data)
        return etl_data

    def store_results(self, etl_data):
        self.send_logstash(etl_data)

    def read_messages(self):
        msgs = self._read_messages(self.broker_uri, self.broker_queue,
                                   cnt=self.msg_limit,
                                   callback=self.process_and_report)
        return msgs

    def serve_forever(self, poll_interval=1.0):
        self.keep_running = True
        while self.keep_running:
            try:
                self.read_messages()
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                break
