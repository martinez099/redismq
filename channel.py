from rqueue import RQueue

import uuid

PATTERN = 'channel_{}:{}'


class Channel(object):
    """
    Channel class.

    A base class for a communication channel.
    """

    def __init__(self, _name, _redis):
        """
        Initialize a newly created Channel object.

        :param _name: The name of the channel.
        :param _redis: A Redis instance.
        """
        self.name = _name
        self.redis = _redis
        self.redis.client_setname(_name)
        self.requests = RQueue(PATTERN.format('requests', self.name), _redis)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.subscriber = None

    def __len__(self):
        return len(self.requests)

    def __del__(self):
        self.close()

    def close(self):
        """
        Close this communication channel.

        :return:
        """
        if self.subscriber:
            self.subscriber.stop()
        self.pubsub.unsubscribe()
        self.pubsub.close()


class Client(Channel):
    """
    Channel Client class.

    An implementation of a communication channel client for Redis.
    """

    def __init__(self, _name, _redis):
        """
        Initialize a newly created Client object.

        :param _name: The name of the channel.
        :param _redis: A Redis instance.
        """
        super(Client, self).__init__(_name, _redis)

    def send_req(self, _value, _id=None):
        """
        Send a request.

        :param _value:
        :param _id:
        :return: The ID of the sent request.
        """
        req_id = str(uuid.uuid4()) if not _id else _id
        if self.redis.set(PATTERN.format('request', req_id), _value):
            if self.requests.push(req_id):
                return req_id

    def recv_rsp(self, _id, _to=0):
        """
        Receive a response. N.B: This is a blocking operation.

        :param _id:
        :param _to: The blocking timeout in seconds. N.B: defaults to 0, i.e. infinite.
        :return:
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        return rsps.bpop(_to)

    def get_rsp(self, _id):
        """
        Get a response. N.B: This is a non-blocking operation.

        :param _id:
        :return:
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        return rsps.pop()

    def ack_rsp(self, _id, _rsp):
        """
        Acknowledge a response when it's done processing.

        :param _id:
        :param _rsp:
        :return:
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        return rsps.ack(_rsp)

    def set_rsp_handler(self, _handler):
        """
        Set a response handler for asynchronous communication.

        :param _handler:
        :return:
        """
        self.pubsub.subscribe(**{PATTERN.format('responses', self.name): _handler})
        if not self.subscriber:
            self.subscriber = self.pubsub.run_in_thread(sleep_time=0.001)

    def unset_rsp_handler(self):
        """
        Unset a response handler.

        :return:
        """
        return self.pubsub.unsubscribe(PATTERN.format('responses', self.name))


class Server(Channel):
    """
    Channel Server class.

    An implementation of a communication channel server for Redis.
    """

    def __init__(self, _name, _redis):
        """
        Initialize a newly created Server object.

        :param _name: The name of the channel.
        :param _redis: A Redis instance.
        """
        super(Server, self).__init__(_name, _redis)

    def recv_req(self, _to=0):
        """
        Receive a request. N.B: This is s blocking operation.

        :param _to: The blocking timeout in seconds. N.B: defaults to 0, i.e. infinite.
        :return: A tuple wrapping the id of the request and the request itself.
        """
        req_id = self.requests.bpop(_to)
        if req_id:
            req = self.redis.get(PATTERN.format('request', req_id))
            return req_id, req

    def ack_req(self, _id):
        """
        Acknowlede a request when it's done processing.

        :param _id:
        :return:
        """
        if self.requests.ack(_id):
            return self.redis.delete(PATTERN.format('request', _id))

    def send_rsp(self, _id, _value):
        """
        Send a response.

        :param _id:
        :param _value:
        :return:
        """
        rsps = RQueue(PATTERN.format('response', self.name) + ':' + _id, self.redis)
        if rsps.push(_value):
            self.redis.publish(PATTERN.format('responses', self.name), _id)
        return True
