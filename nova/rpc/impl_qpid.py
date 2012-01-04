# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright 2011 OpenStack LLC
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

import itertools
import sys
import time
import traceback
import types
import uuid

import eventlet
from eventlet import greenpool
from eventlet import pools
import greenlet

from nova import context
from nova import exception
from nova import flags
from nova.rpc.common import RemoteError, LOG

from qpid.messaging import Connection, Message

# Needed for tests
eventlet.monkey_patch()

FLAGS = flags.FLAGS


class ConsumerBase(object):
    """Consumer base class."""

    def __init__(self, session, callback, address, tag):
        """Declare a queue on an amqp session.

        'session' is the amqp session to use
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the session

        queue name, exchange name, and other kombu options are
        passed in here as a dictionary.
        """
        self.callback = callback
        self.tag = str(tag)
        self.receiver = None
        self.address= address
        self.reconnect(session)

    def reconnect(self, session):
        """Re-declare the queue after a rabbit reconnect"""
        self.session = session
        self.receiver = session.receiver(self.address)
        # WGH - Set the capacity. May want to up this.
        self.receiver.capacity = 1

    def consume(self, *args, **kwargs):
        """Actually declare the consumer on the amqp session.  This will
        start the flow of messages from the queue.  Using the
        Connection.iterconsume() iterator will process the messages,
        calling the appropriate callback.

        If a callback is specified in kwargs, use that.  Otherwise,
        use the callback passed during __init__()

        If kwargs['nowait'] is True, then this call will block until
        a message is read.

        Messages will automatically be acked if the callback doesn't
        raise an exception
        """

        options = {'consumer_tag': self.tag}
        options['nowait'] = kwargs.get('nowait', False)
        callback = kwargs.get('callback', self.callback)
        if not callback:
            raise ValueError("No callback defined")

        message = self.receiver.fetch()
        callback(message)
        # WGH - don't need to do the following:
        # session.acknowledge()

        self.queue.consume(*args, callback=_callback, **options)

    def cancel(self):
        """Cancel the consuming from the queue, if it has started"""
        try:
            self.queue.cancel(self.tag)
        except KeyError, e:
            # NOTE(comstud): Kludge to get around a amqplib bug
            if str(e) != "u'%s'" % self.tag:
                raise
        self.queue = None

class DirectConsumer(ConsumerBase):
    """Queue/consumer class for 'direct'"""

    def __init__(self, session, msg_id, callback, tag, **kwargs):
        """Init a 'direct' queue.

        'session' is the amqp session to use
        'msg_id' is the msg_id to listen on
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the channel

        Other kombu options may be passed
        """
        # Default options
        #options.update(kwargs)

        # WGH -This looks dodgy! exchange, queue and key all have the same name!
        exchange_name = msg_id
        address = exchange_name + '/' + msg_id + '; {create:always, node:{type: topic, x-declare:{durable: True, type: direct, auto-delete: True}}, link:{name:' + msg_id + ', durable: True, x-declare:{durable:False, auto-delete:True}}}'
        super(DirectConsumer, self).__init__(
                session,
                callback,
                address,
                tag)

class TopicConsumer(ConsumerBase):
    """Consumer class for 'topic'"""

    def __init__(self, session, topic, callback, tag, **kwargs):
        """Init a 'topic' queue.

        'session' is the amqp session to use
        'topic' is the topic to listen on
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the session

        Other kombu options may be passed
        """
        # Default options
        # options.update(kwargs)

        exchange_name = FLAGS.control_exchange

        address = exchange_name + '/' + topic + '; {create:always, node:{type: topic, x-declare:{durable: True, auto-delete: True}}, link:{name:' + topic + ', durable: True, x-declare:{durable:False, auto-delete:True}}}'

        super(TopicConsumer, self).__init__(
                session,
                callback,
                address,
                tag)

class FanoutConsumer(ConsumerBase):
    """Consumer class for 'fanout'"""

    def __init__(self, session, topic, callback, tag, **kwargs):
        """Init a 'fanout' queue.

        'session' is the amqp session to use
        'topic' is the topic to listen on
        'callback' is the callback to call when messages are received
        'tag' is a unique ID for the consumer on the session

        Other kombu options may be passed
        """
        unique = uuid.uuid4().hex
        exchange_name = '%s_fanout' % topic
        queue_name = '%s_fanout_%s' % (topic, unique)

        address = exchange_name + '; {create:always, node:{type: topic, x-declare:{durable: False, type: fanout, auto-delete: True}}, link:{name:' + queue_name + ', durable: True, x-declare:{durable:False, auto-delete:True}}}'

        # WGH not sure we use these anymore
        # Default options
        # options.update(kwargs)

        super(FanoutConsumerConsumer, self).__init__(
                session,
                callback,
                address,
                tag )


class Publisher(object):
    """Base Publisher class"""

    def __init__(self, session, exchange_name, address, routing_key, **kwargs):
        """Init the Publisher class with the exchange_name, routing_key,
        and other options
        """
        self.session = session
        self.address = address
        """WGH Not sure we need all of these anymore especially kwargs"""
        self.exchange_name = exchange_name
        self.routing_key = routing_key
        self.kwargs = kwargs
        self.reconnect(session)

    def reconnect(self, session):
        """Re-establish the Sender after a reconnection"""
        self.sender = session.sender(self.address)

    def send(self, msg):
        """Send a message"""
        self.sender.send(msg)


class DirectPublisher(Publisher):
    """Publisher class for 'direct'"""
    def __init__(self, session, msg_id, **kwargs):
        """init a 'direct' publisher.

        Kombu options may be passed as keyword args to override defaults
        """
        exchange = msg_id
        """auto-delete isn't implemented for exchanges in qpid but put in here anyway"""
        address = exchange + '/' + msg_id + '; {create:always, node:{type:topic, x-declare:{durable:False, type:Direct, auto-delete:True}}}'

        """ WGH What to do with any args in kwargs???
        options.update(kwargs)"""

        super(DirectPublisher, self).__init__(session,
                msg_id,
                address,
                msg_id,
                type='direct',
                **options)


class TopicPublisher(Publisher):
    """Publisher class for 'topic'"""
    def __init__(self, session, topic, **kwargs):
        """init a 'topic' publisher.

        Kombu options may be passed as keyword args to override defaults
        """

        exchange = FLAGS.control_exchange
        """auto-delete isn't implemented for exchanges in qpid but put in here anyway"""
        address = exchange + '/' + topic + '; {create:always, node:{type:topic, x-declare:{durable:False, auto-delete:True}}}'

        """ WGH What to do with any args in kwargs???
        options.update(kwargs)"""

        super(TopicPublisher, self).__init__(session,
                exchange,
                address,
                topic,
                type='topic',
                **options)


class FanoutPublisher(Publisher):
    """Publisher class for 'fanout'"""
    def __init__(self, session, topic, **kwargs):
        """init a 'fanout' publisher.

        Kombu options may be passed as keyword args to override defaults
        """
        exchange = '%s_fanout' % topic

        """auto-delete isn't implemented for exchanges in qpid but put in here anyway"""
        self.address = exchange + '; {create:always, node:{type:topic, x-declare:{durable:False, type:fanout, auto-delete:True}}}'

        """ WGH What to do with any args in kwargs???
        options.update(kwargs)"""

        super(FanoutPublisher, self).__init__(session,
                exchange,
                address,
                None,
                type='fanout',
                **options)


class Connection(object):
    """Connection object."""

    def __init__(self):
        self.consumers = []
        self.receivers = {}
        self.consumer_thread = None

        hostname = FLAGS.qpid_hostname
        port = FLAGS.qpid_port
        broker = "localhost:5672" if (len(hostname)<1 and len(port)<1) else hostname + ":" + port
        # Create the connection - this does not open the connection
        self.connection = qpid.messaging.Connection(broker)

        # Check if flags are set and if so set them for the connection
        # before we call open
        if FLAGS.qpid_reconnect:
            self.connection.reconnect = FLAGS.qpid_reconnect
        if FLAGS.qpid_reconnect_timeout:
            self.connection.reconnect_timeout = FLAGS.qpid_reconnect_timeout        
        if FLAGS.qpid_reconnect_limit:
            self.connection.reconnect_limit = FLAGS.qpid_reconnect_limit
        if FLAGS.qpid_reconnect_interval:
            self.connection.reconnect_interval = FLAGS.qpid_reconnect_interval
        if FLAGS.qpid_reconnect_interval:
            self.connection.reconnect_interval_max = FLAGS.qpid_reconnect_interval
        if FLAGS.qpid_reconnect_interval:
        if FLAGS.qpid_reconnect_interval:
            self.connection.hearbeat = FLAGS.qpid_heartbeat
        if FLAGS.qpid_heartbeat:
            self.connection.hearbeat = FLAGS.qpid_heartbeat
        if FLAGS.qpid_protocol:
            self.connection.protocol = FLAGS.qpid_protocol
        if FLAGS.qpid_tcp_nodelay:
            self.connection.tcp_nodelay = FLAGS.qpid_tcp_nodelay
        if FLAGS.qpid_sals_mechanisms:
            self.connection.sasl_mechanisms = FLAGS.qpid_sals_mechanisms
        if FLAGS.qpid_username:
            self.connection.username = FLAGS.qpid_username
        if FLAGS.qpid_password:
            self.connection.password = FLAGS.qpid_password

        # Open is part of reconnect - WGH not sure we need this with the reconnect flags
        self.reconnect()

    def reconnect(self):
        """Handles reconnecting and re-establishing sessions and queues"""
        if self.connection.opened():
            try:
                self.connection.close()
            except self.connection.ConnectionError:
                pass
            time.sleep(1)
        try:
            self.connection.open()
            self.consumer_num = itertools.count(1)  # WGH remove?

         except self.connection.ConnectionError, e:
            # We should only get here if max_retries is set.  We'll go
            # ahead and exit in this case.
            err_str = str(e)
            max_retries = self.reconnect_limit
            LOG.error(_('Unable to connect to AMQP server '
                    'after %(max_retries)d tries: %(err_str)s') % locals())
            sys.exit(1)
        LOG.info(_('Connected to AMQP server on %(hostname)s:%(port)d' %
                self.params))

        for session in self.sessions:
            session = self.connection.session()
        if self.sessions:
            LOG.debug(_("Re-established AMQP Sessions"))

        for consumer in self.consumers:
            consumer.reconnect(self.session) # hmm which session?

        if self.consumers:
            LOG.debug(_("Re-established AMQP queues"))

    def get_session(self):
        """WGH May be useless"""
        return self.session

    def connect_error(self, exc, interval):
        """Callback when there are connection re-tries by kombu"""
        info = self.params.copy()
        info['intv'] = interval
        info['e'] = exc
        LOG.error(_('AMQP server on %(hostname)s:%(port)d is'
                ' unreachable: %(e)s. Trying again in %(intv)d'
                ' seconds.') % info)

    def close(self):
        """Close/release this connection"""
        self.cancel_consumer_thread()
        self.connection.close()
        self.connection = None

    def reset(self):
        """Reset a connection so it can be used again"""
        self.cancel_consumer_thread()
        self.session.close()
        self.session = self.connection.session()
        # work around 'memory' transport bug in 1.1.3
        if self.memory_transport:
            self.session._new_queue('ae.undeliver')
        self.consumers = []

    def declare_consumer(self, consumer_cls, topic, callback):
        """Create a Consumer using the class that was passed in and
        add it to our list of consumers
        """
        consumer = consumer_cls(self.session, topic, callback,
                self.consumer_num.next())
        self.consumers.append(consumer)
        return consumer

    def cancel_consumer_thread(self):
        """Cancel a consumer thread"""
        if self.consumer_thread is not None:
            self.consumer_thread.kill()
            try:
                self.consumer_thread.wait()
            except greenlet.GreenletExit:
                pass
            self.consumer_thread = None

    def publisher_send(self, cls, topic, msg):
        """Send to a publisher based on the publisher class"""
        while True:
            publisher = None
            try:
                publisher = cls(self.session, topic)
                publisher.send(msg)
                return
            except self.connection.connection_errors, e:
                LOG.exception(_('Failed to publish message %s' % str(e)))
                try:
                    self.reconnect()
                    if publisher:
                        publisher.reconnect(self.session)
                except self.connection.connection_errors, e:
                    pass

    def declare_direct_consumer(self, topic, callback):
        """Create a 'direct' queue.
        In nova's use, this is generally a msg_id queue used for
        responses for call/multicall
        """
        self.declare_consumer(DirectConsumer, topic, callback)

    def declare_topic_consumer(self, topic, callback=None):
        """Create a 'topic' consumer."""
        self.declare_consumer(TopicConsumer, topic, callback)

    def declare_fanout_consumer(self, topic, callback):
        """Create a 'fanout' consumer"""
        self.declare_consumer(FanoutConsumer, topic, callback)

    def direct_send(self, msg_id, msg):
        """Send a 'direct' message"""
        self.publisher_send(DirectPublisher, msg_id, msg)

    def topic_send(self, topic, msg):
        """Send a 'topic' message"""
        self.publisher_send(TopicPublisher, topic, msg)

    def fanout_send(self, topic, msg):
        """Send a 'fanout' message"""
        self.publisher_send(FanoutPublisher, topic, msg)

    def consume(self, limit=None):
        """Consume from all queues/consumers"""
        while True:
            try:
                for session in self.sessions:
                    nxt_receiver = session.next_receiver(0)
                    self.receivers[str(nxt_receiver)].consume()
            except MessagingError, m:
                LOG.exception(_('Failed to consume message from queue: '
                        '%s' % m))
                self.reconnect()
                return  # WGH this return good?

    def consume_in_thread(self):
        """Consumer from all queues/consumers in a greenthread"""
        def _consumer_thread():
            try:
                self.consume()
            except greenlet.GreenletExit:
                return
        if self.consumer_thread is None:
            self.consumer_thread = eventlet.spawn(_consumer_thread)
        return self.consumer_thread

    def create_consumer(self, topic, proxy, fanout=False):
        """Create a consumer that calls a method in a proxy object"""
        if fanout:
            consumer = FanoutConsumer(self.session, topic, ProxyCallback(proxy),
                                      self.consumer_num.next())
        else:
            consumer = TopicConsumer(self.session, topic, ProxyCallback(proxy),
                                self.consumer_num.next())
        self.consumers.append(consumer)
        self.receivers[str(consumer.receiver)] = consumer
        return consumer


class Pool(pools.Pool):
    """Class that implements a Pool of Connections."""

    # TODO(comstud): Timeout connections not used in a while
    def create(self):
        LOG.debug('Pool creating new connection')
        return Connection()

# Create a ConnectionPool to use for RPC calls.  We'll order the
# pool as a stack (LIFO), so that we can potentially loop through and
# timeout old unused connections at some point
ConnectionPool = Pool(
        max_size=FLAGS.rpc_conn_pool_size,
        order_as_stack=True)


class ConnectionContext(object):
    """The class that is actually returned to the caller of
    create_connection().  This is a essentially a wrapper around
    Connection that supports 'with' and can return a new Connection or
    one from a pool.  It will also catch when an instance of this class
    is to be deleted so that we can return Connections to the pool on
    exceptions and so forth without making the caller be responsible for
    catching all exceptions and making sure to return a connection to
    the pool.
    """

    def __init__(self, pooled=True):
        """Create a new connection, or get one from the pool"""
        self.connection = None
        if pooled:
            self.connection = ConnectionPool.get()
        else:
            self.connection = Connection()
        self.pooled = pooled

    def __enter__(self):
        """with ConnectionContext() should return self"""
        return self

    def _done(self):
        """If the connection came from a pool, clean it up and put it back.
        If it did not come from a pool, close it.
        """
        if self.connection:
            if self.pooled:
                # Reset the connection so it's ready for the next caller
                # to grab from the pool
                self.connection.reset()
                ConnectionPool.put(self.connection)
            else:
                try:
                    self.connection.close()
                except Exception:
                    # There's apparently a bug in kombu 'memory' transport
                    # which causes an assert failure.
                    # But, we probably want to ignore all exceptions when
                    # trying to close a connection, anyway...
                    pass
            self.connection = None

    def __exit__(self, t, v, tb):
        """end of 'with' statement.  We're done here."""
        self._done()

    def __del__(self):
        """Caller is done with this connection.  Make sure we cleaned up."""
        self._done()

    def close(self):
        """Caller is done with this connection."""
        self._done()

    def __getattr__(self, key):
        """Proxy all other calls to the Connection instance"""
        if self.connection:
            return getattr(self.connection, key)
        else:
            raise exception.InvalidRPCConnectionReuse()


class ProxyCallback(object):
    """Calls methods on a proxy object based on method and args."""

    def __init__(self, proxy):
        self.proxy = proxy
        self.pool = greenpool.GreenPool(FLAGS.rpc_thread_pool_size)

    def __call__(self, message_data):
        """Consumer callback to call a method on a proxy object.

        Parses the message for validity and fires off a thread to call the
        proxy object method.

        Message data should be a dictionary with two keys:
            method: string representing the method to call
            args: dictionary of arg: value

        Example: {'method': 'echo', 'args': {'value': 42}}

        """
        LOG.debug(_('received %s') % message_data)
        ctxt = _unpack_context(message_data)
        method = message_data.get('method')
        args = message_data.get('args', {})
        if not method:
            LOG.warn(_('no method for message: %s') % message_data)
            ctxt.reply(_('No method for message: %s') % message_data)
            return
        self.pool.spawn_n(self._process_data, ctxt, method, args)

    @exception.wrap_exception()
    def _process_data(self, ctxt, method, args):
        """Thread that magically looks for a method on the proxy
        object and calls it.
        """

        node_func = getattr(self.proxy, str(method))
        node_args = dict((str(k), v) for k, v in args.iteritems())
        # NOTE(vish): magic is fun!
        try:
            rval = node_func(context=ctxt, **node_args)
            # Check if the result was a generator
            if isinstance(rval, types.GeneratorType):
                for x in rval:
                    ctxt.reply(x, None)
            else:
                ctxt.reply(rval, None)
            # This final None tells multicall that it is done.
            ctxt.reply(None, None)
        except Exception as e:
            LOG.exception('Exception during message handling')
            ctxt.reply(None, sys.exc_info())
        return


def _unpack_context(msg):
    """Unpack context from msg."""
    context_dict = {}
    for key in list(msg.keys()):
        # NOTE(vish): Some versions of python don't like unicode keys
        #             in kwargs.
        key = str(key)
        if key.startswith('_context_'):
            value = msg.pop(key)
            context_dict[key[9:]] = value
    context_dict['msg_id'] = msg.pop('_msg_id', None)
    LOG.debug(_('unpacked context: %s'), context_dict)
    return RpcContext.from_dict(context_dict)


def _pack_context(msg, context):
    """Pack context into msg.

    Values for message keys need to be less than 255 chars, so we pull
    context out into a bunch of separate keys. If we want to support
    more arguments in rabbit messages, we may want to do the same
    for args at some point.

    """
    context_d = dict([('_context_%s' % key, value)
                      for (key, value) in context.to_dict().iteritems()])
    msg.update(context_d)


class RpcContext(context.RequestContext):
    """Context that supports replying to a rpc.call"""
    def __init__(self, *args, **kwargs):
        msg_id = kwargs.pop('msg_id', None)
        self.msg_id = msg_id
        super(RpcContext, self).__init__(*args, **kwargs)

    def reply(self, *args, **kwargs):
        if self.msg_id:
            msg_reply(self.msg_id, *args, **kwargs)


class MulticallWaiter(object):
    def __init__(self, connection):
        self._connection = connection
        self._iterator = connection.iterconsume()
        self._result = None
        self._done = False

    def done(self):
        self._done = True
        self._iterator.close()
        self._iterator = None
        self._connection.close()

    def __call__(self, data):
        """The consume() callback will call this.  Store the result."""
        if data['failure']:
            self._result = RemoteError(*data['failure'])
        else:
            self._result = data['result']

    def __iter__(self):
        """Return a result until we get a 'None' response from consumer"""
        if self._done:
            raise StopIteration
        while True:
            self._iterator.next()
            result = self._result
            if isinstance(result, Exception):
                self.done()
                raise result
            if result == None:
                self.done()
                raise StopIteration
            yield result


def create_connection(new=True):
    """Create a connection"""
    return ConnectionContext(pooled=not new)


def multicall(context, topic, msg):
    """Make a call that returns multiple times."""
    # Can't use 'with' for multicall, as it returns an iterator
    # that will continue to use the connection.  When it's done,
    # connection.close() will get called which will put it back into
    # the pool
    LOG.debug(_('Making asynchronous call on %s ...'), topic)
    msg_id = uuid.uuid4().hex
    msg.update({'_msg_id': msg_id})
    LOG.debug(_('MSG_ID is %s') % (msg_id))
    _pack_context(msg, context)

    conn = ConnectionContext()
    wait_msg = MulticallWaiter(conn)
    conn.declare_direct_consumer(msg_id, wait_msg)
    conn.topic_send(topic, msg)
    return wait_msg


def call(context, topic, msg):
    """Sends a message on a topic and wait for a response."""
    rv = multicall(context, topic, msg)
    # NOTE(vish): return the last result from the multicall
    rv = list(rv)
    if not rv:
        return
    return rv[-1]


def cast(context, topic, msg):
    """Sends a message on a topic without waiting for a response."""
    LOG.debug(_('Making asynchronous cast on %s...'), topic)
    _pack_context(msg, context)
    with ConnectionContext() as conn:
        conn.topic_send(topic, msg)


def fanout_cast(context, topic, msg):
    """Sends a message on a fanout exchange without waiting for a response."""
    LOG.debug(_('Making asynchronous fanout cast...'))
    _pack_context(msg, context)
    with ConnectionContext() as conn:
        conn.fanout_send(topic, msg)


def msg_reply(msg_id, reply=None, failure=None):
    """Sends a reply or an error on the session signified by msg_id.

    Failure should be a sys.exc_info() tuple.

    """
    with ConnectionContext() as conn:
        if failure:
            message = str(failure[1])
            tb = traceback.format_exception(*failure)
            LOG.error(_("Returning exception %s to caller"), message)
            LOG.error(tb)
            failure = (failure[0].__name__, str(failure[1]), tb)

        try:
            msg = {'result': reply, 'failure': failure}
        except TypeError:
            msg = {'result': dict((k, repr(v))
                            for k, v in reply.__dict__.iteritems()),
                    'failure': failure}
        conn.direct_send(msg_id, msg)
