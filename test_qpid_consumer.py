#!/usr/bin/env python

import sys
import time
import eventlet

from nova import rpc
from nova import flags


flags.FLAGS['rpc_backend'].SetDefault('nova.rpc.impl_qpid')


class PingConsumer(object):
    def __init__(self):
        pass

    def ping(self):
        print "Got a ping"
        return "pong"

    def ping_noreply(self):
        print "Got a ping (no reply)"


def main(argv=None):
    if argv is None:
        argv = sys.argv

    eventlet.monkey_patch()

    consumer = PingConsumer()

    conn = rpc.create_connection(new=True)
    conn.create_consumer("impl_qpid_test", consumer, fanout=False)
    conn.consume_in_thread()

    while True:
        try:
            time.sleep(500)
        except KeyboardInterrupt:
            break

    conn.close()

    return 0


if __name__ == "__main__":
    sys.exit(main())
