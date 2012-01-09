# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2010 United States Government as represented by the
# Administrator of the National Aeronautics and Space Administration.
# All Rights Reserved.
# Copyright 2012, Red Hat, Inc.
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
"""
Unit Tests for remote procedure calls using qpid
"""

import eventlet

eventlet.monkey_patch()

from nova import context
from nova import log as logging
from nova import test
from nova.rpc import impl_qpid
from nova.tests.rpc import common


LOG = logging.getLogger('nova.tests.rpc')


class RpcQpidTestCase(common._BaseRpcTestCase):
    def setUp(self):
        self.rpc = impl_qpid
        super(RpcQpidTestCase, self).setUp()

    def tearDown(self):
        super(RpcQpidTestCase, self).tearDown()
