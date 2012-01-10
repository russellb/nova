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
        self._orig_connection = qpid.messaging.Connection
        self._orig_session = qpid.messaging.Session
        self._orig_sender = qpid.messaging.Sender
        self._orig_receiver = qpid.messaging.Receiver
        super(RpcQpidTestCase, self).setUp()

    def tearDown(self):
        self._restore_orig()
        super(RpcQpidTestCase, self).tearDown()

    def _restore_orig(self):
        qpid.messaging.Connection = self._orig_connection
        qpid.messaging.Session = self._orig_session
        qpid.messaging.Sender = self._orig_sender
        qpid.messaging.Receiver = self._orig_receiver
        self.mocker.ResetAll()

    def test_create_connection(self):
        mock_connection = self.mocker.CreateMock(qpid.messaging.Connection)
        mock_session = self.mocker.CreateMock(qpid.messaging.Session)

        mock_connection.opened().AndReturn(False)
        mock_connection.open()
        mock_connection.session().AndReturn(mock_session)
        mock_connection.close()

        qpid.messaging.Connection = lambda *_x, **_y: mock_connection
        qpid.messaging.Session = lambda *_x, **_y: mock_session

        self.mocker.ReplayAll()

        try:
            connection = impl_qpid.create_connection()
            connection.close()

            self.mocker.VerifyAll()
        finally:
            self._restore_orig()

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

        qpid.messaging.Connection = lambda *_x, **_y: mock_connection
        qpid.messaging.Session = lambda *_x, **_y: mock_session
        qpid.messaging.Receiver = lambda *_x, **_y: mock_receiver

        self.mocker.ReplayAll()

        try:
            connection = impl_qpid.create_connection()
            consumer = connection.create_consumer("impl_qpid_test",
                                                  lambda *_x, **_y: None,
                                                  fanout)
            connection.close()

            self.mocker.VerifyAll()
        finally:
            self._restore_orig()

    def test_create_consumer(self):
        self._test_create_consumer(fanout=False)

    def test_create_consumer_fanout(self):
        self._test_create_consumer(fanout=True)

    def _test_cast(self, fanout):
        mock_connection = self.mocker.CreateMock(qpid.messaging.Connection)
        mock_session = self.mocker.CreateMock(qpid.messaging.Session)
        mock_sender = self.mocker.CreateMock(qpid.messaging.Sender)

        mock_connection.opened().AndReturn(False)
        mock_connection.open()
        mock_connection.session().AndReturn(mock_session)
        if fanout:
            expected_address = 'impl_qpid_test_fanout ; ' \
                '{"node": {"x-declare": {"auto-delete": true, ' \
                '"durable": false, "type": "fanout"}, ' \
                '"type": "topic"}, "create": "always"}'
        else:
            expected_address = 'nova/impl_qpid_test ; {"node": {"x-declare": '\
                '{"auto-delete": true, "durable": false}, "type": "topic"}, ' \
                '"create": "always"}'
        mock_session.sender(expected_address).AndReturn(mock_sender)
        mock_sender.send(mox.IgnoreArg())
        # This is a pooled connection, so instead of closing it, it gets reset,
        # which is just creating a new session on the connection.
        mock_session.close()
        mock_connection.session().AndReturn(mock_session)

        qpid.messaging.Connection = lambda *_x, **_y: mock_connection
        qpid.messaging.Session = lambda *_x, **_y: mock_session
        qpid.messaging.Sender = lambda *_x, **_y: mock_sender

        self.mocker.ReplayAll()

        try:
            ctx = context.RequestContext("user", "project")

            if fanout:
                impl_qpid.fanout_cast(ctx, "impl_qpid_test",
                               {"method": "test_method", "args": {}})
            else:
                impl_qpid.cast(ctx, "impl_qpid_test",
                               {"method": "test_method", "args": {}})

            self.mocker.VerifyAll()
        finally:
            if impl_qpid.ConnectionContext._connection_pool.free():
                # Pull the mock connection object out of the connection pool so
                # that it doesn't mess up other test cases.
                impl_qpid.ConnectionContext._connection_pool.get()
            self._restore_orig()

    def test_cast(self):
        self._test_cast(fanout=False)

    def test_fanout_cast(self):
        self._test_cast(fanout=True)

    def _test_call(self, multi):
        mock_connection = self.mocker.CreateMock(qpid.messaging.Connection)
        mock_session = self.mocker.CreateMock(qpid.messaging.Session)
        mock_sender = self.mocker.CreateMock(qpid.messaging.Sender)
        mock_receiver = self.mocker.CreateMock(qpid.messaging.Receiver)

        mock_connection.opened().AndReturn(False)
        mock_connection.open()
        mock_connection.session().AndReturn(mock_session)
        rcv_addr = mox.Regex(r'^.*/.* ; {"node": {"x-declare": {"auto-delete":'
                   ' true, "durable": true, "type": "direct"}, "type": '
                   '"topic"}, "create": "always", "link": {"x-declare": '
                   '{"auto-delete": true, "durable": false}, "durable": '
                   'true, "name": ".*"}}')
        mock_session.receiver(rcv_addr).AndReturn(mock_receiver)
        mock_receiver.capacity = 1
        send_addr = 'nova/impl_qpid_test ; {"node": {"x-declare": ' \
            '{"auto-delete": true, "durable": false}, "type": "topic"}, ' \
            '"create": "always"}'
        mock_session.sender(send_addr).AndReturn(mock_sender)
        mock_sender.send(mox.IgnoreArg())

        mock_session.next_receiver().AndReturn(mock_receiver)
        mock_receiver.fetch().AndReturn(qpid.messaging.Message(
                        {"result": "foo", "failure": False, "ending": False}))
        if multi:
            mock_session.next_receiver().AndReturn(mock_receiver)
            mock_receiver.fetch().AndReturn(
                            qpid.messaging.Message(
                                {"result": "bar", "failure": False,
                                 "ending": False}))
            mock_session.next_receiver().AndReturn(mock_receiver)
            mock_receiver.fetch().AndReturn(
                            qpid.messaging.Message(
                                {"result": "baz", "failure": False,
                                 "ending": False}))
        mock_session.next_receiver().AndReturn(mock_receiver)
        mock_receiver.fetch().AndReturn(qpid.messaging.Message(
                        {"failure": False, "ending": True}))
        mock_session.close()
        mock_connection.session().AndReturn(mock_session)

        qpid.messaging.Connection = lambda *_x, **_y: mock_connection
        qpid.messaging.Session = lambda *_x, **_y: mock_session
        qpid.messaging.Sender = lambda *_x, **_y: mock_sender
        qpid.messaging.Receiver = lambda *_x, **_y: mock_receiver

        self.mocker.ReplayAll()

        try:
            ctx = context.RequestContext("user", "project")

            if multi:
                method = impl_qpid.multicall
            else:
                method = impl_qpid.call

            res = method(ctx, "impl_qpid_test",
                           {"method": "test_method", "args": {}})

            if multi:
                self.assertEquals(list(res), ["foo", "bar", "baz"])
            else:
                self.assertEquals(res, "foo")

            self.mocker.VerifyAll()
        finally:
            if impl_qpid.ConnectionContext._connection_pool.free():
                # Pull the mock connection object out of the connection pool so
                # that it doesn't mess up other test cases.
                impl_qpid.ConnectionContext._connection_pool.get()
            self._restore_orig()

    def test_call(self):
        self._test_call(multi=False)

    def test_multicall(self):
        self._test_call(multi=True)


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
