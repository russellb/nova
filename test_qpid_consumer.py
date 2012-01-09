#!/usr/bin/env python

import sys
import time
import eventlet
import uuid

from nova import rpc
from nova import flags
from nova import log as logging


flags.FLAGS['rpc_backend'].SetDefault('nova.rpc.impl_qpid')


class PingConsumer(object):
    def __init__(self, consumer_type):
        self.consumer_type = consumer_type
        self.consumer_uuid = str(uuid.uuid4())

    def ping(self, context, **kwargs):
        print "[%s] [%s] Got a ping" % (self.consumer_type, self.consumer_uuid)
        return "pong"

    def ping_multicall(self, context, **kwargs):
        print "[%s] [%s] Got a ping (multicall)" % (self.consumer_type, self.consumer_uuid)
        context.reply("pong (1 of 3)")
        context.reply("pong (2 of 3)")
        context.reply("pong (3 of 3)")
        context.reply(ending=True)

    def ping_multicall_return_nones(self, context, **kwargs):
        print "[%s] [%s] Got a ping (multicall), returning None 3 times" % (self.consumer_type, self.consumer_uuid)
        context.reply(None)
        context.reply(None)
        context.reply(None)
        context.reply(ending=True)

    def ping_noreply(self, context, **kwargs):
        print "[%s] [%s] Got a ping (no reply)" % (self.consumer_type,
                                                   self.consumer_uuid)


def main(argv=None):
    if argv is None:
        argv = sys.argv

    eventlet.monkey_patch()

    logging.setup()

    consumer = PingConsumer("topic")
    fanout_consumer = PingConsumer("fanout")

    conn = rpc.create_connection(new=True)
    conn.create_consumer("impl_qpid_test", consumer, fanout=False)
    conn.create_consumer("impl_qpid_test", fanout_consumer, fanout=True)
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
