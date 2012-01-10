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

import mox
import qpid

from nova import context
from nova import log as logging
from nova import test
from nova.rpc import impl_qpid
from nova.tests.rpc import common


LOG = logging.getLogger('nova.tests.rpc')


class RpcQpidTestCase(test.TestCase):
    def setUp(self):
        self.mocker = mox.Mox()
        super(RpcQpidTestCase, self).setUp()

    def tearDown(self):
        self.mocker.ResetAll()
        super(RpcQpidTestCase, self).tearDown()

    def test_create_connection(self):
        mock_connection = self.mocker.CreateMock(qpid.messaging.Connection)
        mock_session = self.mocker.CreateMock(qpid.messaging.Session)

        mock_connection.opened().AndReturn(False)
        mock_connection.open()
        mock_connection.session().AndReturn(mock_session)
        mock_connection.close()

        def _fake_connection(*args, **kwargs):
            return mock_connection

        def _fake_session(*args, **kwargs):
            return mock_session

        orig_connection = qpid.messaging.Connection
        qpid.messaging.Connection = _fake_connection
        orig_session = qpid.messaging.Session
        qpid.messaging.Session = _fake_session

        self.mocker.ReplayAll()

        connection = impl_qpid.create_connection()
        connection.close()

        self.mocker.VerifyAll()

        qpid.messaging.Connection = orig_connection
        qpid.messaging.Session = orig_session

    def _test_create_consumer(self, fanout):
        mock_connection = self.mocker.CreateMock(qpid.messaging.Connection)
        mock_session = self.mocker.CreateMock(qpid.messaging.Session)
        mock_receiver = self.mocker.CreateMock(qpid.messaging.Receiver)

        mock_connection.opened().AndReturn(False)
        mock_connection.open()
        mock_connection.session().AndReturn(mock_session)
        if fanout:
            # The link name includes a UUID, so match it with a regex.
            expected_address = mox.Regex(r'^impl_qpid_test_fanout ; '
                '{"node": {"x-declare": {"auto-delete": true, "durable": '
                'false, "type": "fanout"}, "type": "topic"}, "create": '
                '"always", "link": {"x-declare": {"auto-delete": true, '
                '"durable": false}, "durable": true, "name": '
                '"impl_qpid_test_fanout_.*"}}$')
        else:
            expected_address = 'nova/impl_qpid_test ; {"node": {"x-declare": '\
                '{"auto-delete": true, "durable": true}, "type": "topic"}, ' \
                '"create": "always", "link": {"x-declare": {"auto-delete": ' \
                'true, "exclusive": false, "durable": false}, "durable": ' \
                'true, "name": "impl_qpid_test"}}'
        mock_session.receiver(expected_address).AndReturn(mock_receiver)
        mock_receiver.capacity = 1
        mock_connection.close()

        def _fake_connection(*args, **kwargs):
            return mock_connection

        def _fake_session(*args, **kwargs):
            return mock_session

        def _fake_receiver(*args, **kwargs):
            return mock_receiver

        orig_connection = qpid.messaging.Connection
        qpid.messaging.Connection = _fake_connection
        orig_session = qpid.messaging.Session
        qpid.messaging.Session = _fake_session
        orig_receiver = qpid.messaging.Receiver
        qpid.messaging.Session = _fake_session

        self.mocker.ReplayAll()

        def _proxy(*args, **kwargs):
            pass

        connection = impl_qpid.create_connection()
        consumer = connection.create_consumer("impl_qpid_test",
                                              lambda *_x, **_y: None,
                                              fanout=fanout)
        connection.close()

        self.mocker.VerifyAll()

        qpid.messaging.Connection = orig_connection
        qpid.messaging.Session = orig_session
        qpid.messaging.Receiver = orig_receiver

    def test_create_consumer(self):
        self._test_create_consumer(fanout=False)

    def test_create_consumer_fanout(self):
        self._test_create_consumer(fanout=True)

#
# Qpid does not have a handy in-memory transport like kombu, so it's not
# terribly straight forward to take advantage of the common unit tests.
# However, at least at the time of this writing, the common unit tests all pass
# with qpidd running.
#
# class RpcQpidCommonTestCase(common._BaseRpcTestCase):
#     def setUp(self):
#         self.rpc = impl_qpid
#         super(RpcQpidCommonTestCase, self).setUp()
#
#     def tearDown(self):
#         super(RpcQpidCommonTestCase, self).tearDown()
#
