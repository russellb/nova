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
Server side of a base rpc API for all services.
'''

from nova.openstack.common import jsonutils


BASE_RPC_NAMESPACE = 'baseapi'


class BaseRPCAPI(object):
    RPC_API_NAMESPACE = BASE_RPC_NAMESPACE
    RPC_API_VERSION = '1.0'

    def __init__(self, service_name):
        self.service_name = service_name

    def ping(self, context, arg):
        resp = {'service': self.service_name, 'arg': arg}
        return jsonutils.to_primitive(resp)
