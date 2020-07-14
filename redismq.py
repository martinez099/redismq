import uuid

from redis import StrictRedis

from rqueue.rqueue import RQueue

PATTERN = 'channel_{}:{}'


class Channel(object):
    """
    Channel class.

    A base class for a communication channel.
    """

    def __init__(self, _name, _redis_host, _redis_port):
        """
        :param _name: The name of the channel.
        :param _redis_host: The Redis host.
        :param _redis_port: The Redis port.
        """
        self.name = _name
        self.redis = StrictRedis(decode_responses=True, host=_redis_host, port=_redis_port)
        self.redis.client_setname(_name)
        self.messages = RQueue(PATTERN.format('messages', self.name), self.redis)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.subscriber = None

    def __len__(self):
        return len(self.messages)

    def __del__(self):
        self.close()

    def close(self):
        """
        Close this communication channel.

        :return: None
        """
        if self.subscriber:
            self.subscriber.stop()
        self.pubsub.unsubscribe()
        self.pubsub.close()


class Producer(Channel):
    """
    Producer class.

    An implementation of a message producer for Redis.
    """

    def __init__(self, _name, _redis_host='localhost', _redis_port=6379):
        """
        :param _name: The name of the channel.
        :param _redis_host: The Redis host.
        :param _redis_port: The Redis port.
        """
        super(Producer, self).__init__(_name, _redis_host, _redis_port)

    def send_msg(self, _value, _id=None):
        """
        Send a message.

        :param _value: The payload of the message.
        :param _id: An optional message ID.
        :return: The ID of the sent message on success, else None.
        """
        msg_id = str(uuid.uuid4()) if not _id else _id
        with self.redis.pipeline() as pipe:
            pipe.set(PATTERN.format('message', msg_id), _value)
            self.messages.push(msg_id, pipe)
            ok1, ok2 = pipe.execute()

        return msg_id if ok1 and ok2 else None

    def recv_rsp(self, _id, _to):
        """
        Receive a response. N.B: This is a blocking operation.

        :param _id: The resonse ID, i.e. the message ID.
        :param _to: The blocking timeout in seconds.
        :return: The payload of the response, or None.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)

        return rsps.bpop(_to)

    def get_rsp(self, _id):
        """
        Get a response. N.B: This is a non-blocking operation.

        :param _id: The response ID, i.e. the message ID.
        :return: The payload of the response.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)

        return rsps.pop()

    def ack_rsp(self, _id, _payload):
        """
        Acknowledge a response when it's done processing.

        :param _id: The response ID.
        :param _payload: The payload of the response.
        :return: Success.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)

        return rsps.ack(_payload)

    def set_rsp_handler(self, _handler):
        """
        Set a response handler for asynchronous communication.

        :param _handler: The handler function.
        """
        self.pubsub.subscribe(**{PATTERN.format('responses', self.name): _handler})
        if self.subscriber:
            return

        self.subscriber = self.pubsub.run_in_thread(sleep_time=0.001)

    def unset_rsp_handlers(self):
        """
        Unset all response handlers.

        :return: Success.
        """
        self.subscriber.stop()
        self.subscriber = None

        return self.pubsub.unsubscribe(PATTERN.format('responses', self.name))


class Consumer(Channel):
    """
    Consumer class.

    An implementation of a message consumer for Redis.
    """

    def __init__(self, _name, _redis_host='localhost', _redis_port=6379):
        """
        :param _name: The name of the channel.
        :param _redis_host: The Redis host.
        :param _redis_port: The Redis port.
        """
        super(Consumer, self).__init__(_name, _redis_host, _redis_port)

    def recv_msg(self, _to=0):
        """
        Receive a message. N.B: This is a blocking operation.

        :param _to: The blocking timeout in seconds. N.B: Defaults to 0, i.e. infinite.
        :return: A tuple wrapping the message ID and the message itself, or (None, None)
        """
        msg_id = self.messages.bpop(_to)
        if not msg_id:
            return None, None

        return msg_id, self.redis.get(PATTERN.format('message', msg_id))

    def get_msg(self):
        """
        Get the next message in the queue. N.B: This is a non-blocking operation.

        :return: A tuple wrapping the message ID and the message payload, or None.
        """
        msg_id = self.messages.pop()
        if not msg_id:
            return

        return msg_id, self.redis.get(PATTERN.format('message', msg_id))

    def ack_msg(self, _id):
        """
        Acknowlede a message when it's done processing.

        :param _id: The message ID.
        :return: Success.
        """
        if not self.messages.ack(_id):
            return

        return self.redis.delete(PATTERN.format('message', _id))

    def send_rsp(self, _id, _value, _ttl=60):
        """
        Send a response back to the producer.

        :param _id: The response ID, should be the same as the message ID.
        :param _value: The response payload.
        :param _ttl: Optional TTL in seconds, defaults to 60.
        :return: Success.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis, _ttl)
        if not rsps.push(_value):
            return

        return self.redis.publish(PATTERN.format('responses', self.name), _id)
