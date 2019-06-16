PATTERN = 'rqueue_{}:{}'


class RQueue(object):
    """
    RQueue class.

    An implementation of a reliable queue for Redis, see https://redis.io/commands/rpoplpush.
    """

    def __init__(self, _name, _redis):
        """
        :param _name: The name of the queue.
        :param _redis: An Redis instance.
        """
        self.name = _name
        self.redis = _redis

    def __len__(self):
        return int(self.redis.llen(PATTERN.format('access', self.name))) + \
            int(self.redis.llen(PATTERN.format('process', self.name)))

    def push(self, _value):
        """
        Push a value onto the back of the queue.

        :param _value: The value to put into the queue.
        :return: The amount of values in the list.
        """
        return self.redis.lpush(PATTERN.format('access', self.name), _value)

    def pop(self):
        """
        Pop the next value from the top of the queue. N.B: This is a non-blocking operation.

        :return: The next value in the queue, else None.
        """
        return self.redis.rpoplpush(PATTERN.format('access', self.name), PATTERN.format('process', self.name))

    def bpop(self, _to=0):
        """
        Pop the next value from the top of the queue. N.B: This is a blocking operation iff the queue is empty.

        :_to: Blocking timeout in seconds. N.B: defaults to 0, i.e. infinite
        :return: The next value in the queue, else None.
        """
        return self.redis.brpoplpush(PATTERN.format('access', self.name), PATTERN.format('process', self.name), _to)

    def ack(self, _value):
        """
        Acknowledge a value from the queue, i.e. successfully processed.

        :param _value: The value to be acknowledged.
        :return: Success.
        """
        return bool(self.redis.lrem(PATTERN.format('process', self.name), 1, _value))
