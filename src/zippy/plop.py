from .connection import Connection
from kombu.mixins import ConsumerMixin
import time
import logging


class PlopService(ConsumerMixin):
    NAME = 'plop'
    KOMBU_URI = "redis://127.0.0.1:6379"
    KOMBU_Q = "logstash-ingest"
    LOGSTASH_URI = "udp://127.0.0.1:5002"
    LOGSTASH_QUEUE = "logstash-results"
    SYSLOG_MSG_TYPE = {
        0: "EMERGENCY",
        1: "ALERT",
        2: "CRITICAL",
        3: "ERROR",
        4: "WARNING",
        5: "NOTICE",
        6: "INFORMATIONAL",
        7: "DEBUG",
    }

    def __init__(self, broker_uri=KOMBU_URI, broker_queue=KOMBU_Q,
                 logstash_uri=LOGSTASH_URI,
                 name=NAME, msg_limit=100):

        self.broker_uri = broker_uri
        self.queue = broker_queue
        self.logstash_uri = logstash_uri

        self.conn = Connection.create_connection(broker_uri, broker_queue)
        x = Connection.create_connection(logstash_uri, None)
        self.logstash_conn = x

        self.keep_running = False
        self.msg_limit = msg_limit

    def send_results(self, etl_data):
        print(etl_data)
        m = "Sending results to logstash"
        logging.debug(m)
        if not self.logstash_conn.send_msg(etl_data):
            logging.debug("Failed to send the logs to logstash")

    def read_messages(self):
        msgs = self.conn.read_messages(cnt=self.msg_limit,
                                       callback=self.send_results)
        return msgs

    def serve_forever(self, poll_interval=1.0):
        self.keep_running = True
        while self.keep_running:
            try:
                self.read_messages()
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                break
