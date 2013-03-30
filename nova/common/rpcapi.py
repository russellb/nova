#
# Copyright 2013 Red Hat, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#

'''
Client side of a base rpc API for all services.
'''

import nova.common.base
import nova.openstack.common.rpc.proxy as rpc_proxy


class BaseAPI(rpc_proxy.RpcProxy):
    '''Client side of the base rpc API.

    API version history:

        1.0 - Initial version.
    '''

    #
    # NOTE(russellb): This is the default minimum version that the server
    # (manager) side must implement unless otherwise specified using a version
    # argument to self.call()/cast()/etc. here.  It should be left as X.0 where
    # X is the current major API version (1.0, 2.0, ...).  For more information
    # about rpc API versioning, see the docs in
    # openstack/common/rpc/dispatcher.py.
    #
    BASE_RPC_API_VERSION = '1.0'

    def __init__(self, topic):
        super(ConsoleAuthAPI, self).__init__(topic=topic
                default_version=self.BASE_RPC_API_VERSION)

    def ping(self, context, arg, timeout=None):
        arg_p = jsonutils.to_primitive(arg)
        msg = self.make_msg('ping', nova.common.base.BASE_RPC_NAMESPACE,
                            arg=arg_p)
        return self.call(context, msg, timeout=timeout)
