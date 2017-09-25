import logging
import argparse
import sys

from zippy.plop import PlopService as Plop


parser = argparse.ArgumentParser(
                      description='Start Kombu to logstash captures.')
#  Hitter stuff
parser.add_argument('-name', type=str, default=Plop.NAME,
                    help='name of service')
parser.add_argument('-broker_uri', type=str,
                    default=Plop.BROKER_URI,
                    help='kombu queue address')
parser.add_argument('-broker_queue', type=str,
                    default=Plop.BROKER_QUEUE,
                    help='logstash queue to read from')
parser.add_argument('-msg_limit', type=int, default=100,
                    help='limit the number of messages read')
parser.add_argument('-logstash_uri', type=str, default=Plop.LOGSTASH_URI,
                    help='logstash uri (udp, tcp, redis, or amqp)')

V = 'log levels: INFO: %d, DEBUG: %d, WARRNING: %d' % (logging.INFO,
                                                       logging.DEBUG,
                                                       logging.WARNING)
parser.add_argument('-log_level', type=int, default=logging.DEBUG,
                    help=V)


if __name__ == "__main__":
    args = parser.parse_args()

    logging.getLogger().setLevel(args.log_level)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    logging.getLogger().addHandler(ch)

    service = Plop(broker_uri=args.broker_uri,
                   broker_queue=args.broker_queue,
                   logstash_uri=args.logstash_uri,
                   name=args.name, msg_limit=args.msg_limit)

    try:
        logging.debug("Starting the syslog listener")
        service.serve_forever(poll_interval=0.5)
    except (IOError, SystemExit):
        raise
    except KeyboardInterrupt:
        raise
