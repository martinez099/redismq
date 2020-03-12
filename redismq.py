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
        self.requests = RQueue(PATTERN.format('requests', self.name), self.redis)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.subscriber = None

    def __len__(self):
        return len(self.requests)

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
    Producer Channel class.

    An implementation of a communication channel producer for Redis.
    """

    def __init__(self, _name, _redis_host='localhost', _redis_port=6379):
        """
        :param _name: The name of the channel.
        :param _redis_host: The Redis host.
        :param _redis_port: The Redis port.
        """
        super(Producer, self).__init__(_name, _redis_host, _redis_port)

    def send_req(self, _value, _id=None):
        """
        Send a request.

        :param _value: The payload of the request.
        :param _id: The ÍD of the request, optional.
        :return: The ID of the sent request, or None.
        """
        req_id = str(uuid.uuid4()) if not _id else _id
        with self.redis.pipeline() as pipe:
            pipe.set(PATTERN.format('request', req_id), _value)
            self.requests.push(req_id, pipe)
            ok1, ok2 = pipe.execute()
            return req_id if ok1 and ok2 else None

    def recv_rsp(self, _id, _to=0):
        """
        Receive a response. N.B: This is a blocking operation.

        :param _id: The ID of the resonse, i.e. the ID of the request.
        :param _to: The blocking timeout in seconds. N.B: defaults to 0, i.e. infinite.
        :return: The payload of the response, or None.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        return rsps.bpop(_to)

    def get_rsp(self, _id):
        """
        Get a response. N.B: This is a non-blocking operation.

        :param _id: The id of the response, i.e. the ID of the request.
        :return: The payload of the response.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        return rsps.pop()

    def ack_rsp(self, _id, _payload):
        """
        Acknowledge a response when it's done processing.

        :param _id: The ID of the response.
        :param _payload: The payload of the response.
        :return: Success.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        return rsps.ack(_payload)

    def set_rsp_handler(self, _handler):
        """
        Set a response handler for asynchronous communication.

        :param _handler: The handler function.
        :return: None
        """
        self.pubsub.subscribe(**{PATTERN.format('responses', self.name): _handler})
        if not self.subscriber:
            self.subscriber = self.pubsub.run_in_thread(sleep_time=0.001)

    def unset_rsp_handler(self):
        """
        Unset a response handler.

        :return: Success.
        """
        self.subscriber = None
        return self.pubsub.unsubscribe(PATTERN.format('responses', self.name))


class Consumer(Channel):
    """
    Consumer Channel class.

    An implementation of a communication channel consumer for Redis.
    """

    def __init__(self, _name, _redis_host='localhost', _redis_port=6379):
        """
        :param _name: The name of the channel.
        :param _redis_host: The Redis host.
        :param _redis_port: The Redis port.
        """
        super(Consumer, self).__init__(_name, _redis_host, _redis_port)

    def recv_req(self, _to=0):
        """
        Receive a request. N.B: This is a blocking operation.

        :param _to: The blocking timeout in seconds. N.B: defaults to 0, i.e. infinite.
        :return: A tuple wrapping the id of the request and the request itself, or None
        """
        req_id = self.requests.bpop(_to)
        if req_id:
            return req_id, self.redis.get(PATTERN.format('request', req_id))
        return None, None

    def get_req(self):
        """
        Get the next request. N.B: This is a non-blocking operation.

        :return: A tuple wrapping the id of the request and the request itself, or None.
        """
        req_id = self.requests.pop()
        if req_id:
            return req_id, self.redis.get(PATTERN.format('request', req_id))

    def ack_req(self, _id):
        """
        Acknowlede a request when it's done processing.

        :param _id: The ID of the request.
        :return: Success
        """
        if self.requests.ack(_id):
            return bool(self.redis.delete(PATTERN.format('request', _id)))
        return False

    def send_rsp(self, _id, _value):
        """
        Send a response back to the consumer.

        :param _id: The ID of the response, should be the same ID of the request.
        :param _value: The response payload.
        :return: Success.
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        if rsps.push(_value):
            self.redis.publish(PATTERN.format('responses', self.name), _id)
            return True
        return False
