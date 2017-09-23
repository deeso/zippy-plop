import logging
import argparse
import sys

from zippy.plop import PlopService as Plop


logging.getLogger().setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(message)s')
ch.setFormatter(formatter)
logging.getLogger().addHandler(ch)

parser = argparse.ArgumentParser(
                      description='Start Kombu to logstash captures.')
#  Hitter stuff
parser.add_argument('-name', type=str, default=Plop.NAME,
                    help='name of service')
parser.add_argument('-broker_uri', type=str,
                    default=Plop.KOMBU_URI,
                    help='kombu queue address')
parser.add_argument('-broker_queue', type=str,
                    default=Plop.LOGSTASH_QUEUE,
                    help='kombu queue name to read from')

parser.add_argument('-logstash_uri', type=str, default=Plop.KOMBU_URI,
                    help='logstash uri (udp, tcp, redis, or amqp)')

V = 'log levels: INFO: %d, DEBUG: %d, WARRNING: %d' % (logging.INFO,
                                                       logging.DEBUG,
                                                       logging.WARNING)
parser.add_argument('-log_level', type=int, default=logging.ALERT,
                    help=V)


if __name__ == "__main__":
    args = parser.parse_args()

    service = Plop(broker_uri=args.broker_uri,
                   broker_queue=args.broker_queue,
                   logstash_uri=args.logstash_uri,
                   name=args.name)

    try:
        logging.debug("Starting the syslog listener")
        service.serve_forever(poll_interval=0.5)
    except (IOError, SystemExit):
        raise
    except KeyboardInterrupt:
        raise
