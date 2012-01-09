#!/usr/bin/env python

import sys
import time
import eventlet

from nova import rpc
from nova import flags
from nova import context
from nova import log as logging


flags.FLAGS['rpc_backend'].SetDefault('nova.rpc.impl_qpid')


def main(argv=None):
    if argv is None:
        argv = sys.argv

    eventlet.monkey_patch()

    logging.setup()

    ctx = context.RequestContext("user", "project")

    print "Testing rpc.cast() ..."
    rpc.cast(ctx, "impl_qpid_test", {"method": "ping_noreply", "args":{}})

    print "Testing rpc.fanout_cast() ..."
    rpc.fanout_cast(ctx, "impl_qpid_test",
                    {"method": "ping_noreply", "args":{}})

    print "Testing rpc.call() ... response: ",
    res = rpc.call(ctx, "impl_qpid_test", {"method": "ping", "args":{}})
    print res

    print "Testing rpc.multicall() ... responses: ",
    res = rpc.multicall(ctx, "impl_qpid_test", {"method": "ping_multicall", "args":{}})
    for r in res:
        print r,
    print

    return 0


if __name__ == "__main__":
    sys.exit(main())
