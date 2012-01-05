#!/usr/bin/env python

import sys
import time

from nova import rpc
from nova import flags
from nova import context


flags.FLAGS['rpc_backend'].SetDefault('nova.rpc.impl_qpid')


def main(argv=None):
    if argv is None:
        argv = sys.argv

    ctx = context.RequestContext("user", "project")

    rpc.cast(ctx, "impl_qpid_test", {"method": "ping_noreply", "args":{}})

    return 0


if __name__ == "__main__":
    sys.exit(main())
