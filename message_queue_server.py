import logging
import os
import signal
import time
from concurrent import futures

import grpc

from message_queue_pb2 import SendResponse, ReceiveResponse, GetResponse, AcknowledgeResponse
from message_queue_pb2_grpc import MessageQueueServicer, add_MessageQueueServicer_to_server

from redismq import Producer, Consumer


class MessageQueue(MessageQueueServicer):
    """
    Message Queue class.
    """

    def __init__(self):
        self.subscribers = {}
        self.channels = {}

    def _get_channel(self, service_name, func_name, channel_class):
        """
        Get or create communication channel.

        :param service_name: The name of the service.
        :param func_name: The name of the function.
        :param channel_class: The channel class.
        :return: The communication channel.
        """
        channel_name = '{}.{}'.format(service_name, func_name)
        channel = self.channels.get((channel_name, channel_class))

        if not channel:
            channel = channel_class(channel_name, MESSAGE_QUEUE_REDIS_HOST, MESSAGE_QUEUE_REDIS_PORT)
            self.channels[(channel_name, channel_class)] = channel

        return channel

    def send_msg(self, request, context):
        """
        Send a message.

        :param request: The client request.
        :param context: The client context.
        :return: The message ID.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        msg_id = channel.send_msg(request.payload)

        return SendResponse(msg_id=msg_id)

    def recv_rsp(self, request, context):
        """
        Receive a response.

        :param request: The client request.
        :param context: The client context.
        :return: The response payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        response = channel.recv_rsp(request.msg_id, request.timeout)

        return ReceiveResponse(payload=response, msg_id=request.msg_id)

    def get_rsp(self, request, context):
        """
        Get a response.

        :param request: The client request.
        :param context: The client context.
        :return: The response payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        response = channel.get_rsp(request.msg_id)

        return GetResponse(payload=response, msg_id=request.msg_id)

    def ack_rsp(self, request, context):
        """
        Acknowledge a response.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        success = channel.ack_rsp(request.msg_id, request.payload)

        return AcknowledgeResponse(success=success)

    def recv_msg(self, request, context):
        """
        Receive a message.

        :param request: The client request.
        :param context: The client context.
        :return: The messager payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        (msg_id, msg) = channel.recv_msg(request.timeout)

        return ReceiveResponse(payload=msg, msg_id=msg_id)

    def get_msg(self, request, context):
        """
        Get a message.

        :param request: The client request.
        :param context: The client context.
        :return: The message payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        (msg_id, msg) = channel.get_msg()

        return GetResponse(payload=msg, msg_id=msg_id)

    def ack_msg(self, request, context):
        """
        Acknowledge a message.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        acknowleged = channel.ack_msg(request.msg_id)

        return AcknowledgeResponse(success=acknowleged)

    def send_rsp(self, request, context):
        """
        Send a response.

        :param request: The client request.
        :param context: The client context.
        :return: The ID of the response.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        sent = channel.send_rsp(request.msg_id, request.payload)

        return SendResponse(msg_id=request.msg_id if sent else None)


MESSAGE_QUEUE_REDIS_HOST = os.getenv('MESSAGE_QUEUE_REDIS_HOST', 'localhost')
MESSAGE_QUEUE_REDIS_PORT = int(os.getenv('MESSAGE_QUEUE_REDIS_PORT', '6379'))
MESSAGE_QUEUE_LISTEN_PORT = os.getenv('MESSAGE_QUEUE_LISTEN_PORT', '50051')
MESSAGE_QUEUE_MAX_WORKERS = int(os.getenv('MESSAGE_QUEUE_MAX_WORKERS', '10'))

MESSAGE_QUEUE_ADDRESS = '[::]:{}'.format(MESSAGE_QUEUE_LISTEN_PORT)
MESSAGE_QUEUE_SLEEP_INTERVAL = 1
MESSAGE_QUEUE_GRACE_INTERVAL = 0
MESSAGE_QUEUE_RUNNING = True


def serve():
    """
    Run the gRPC server.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=MESSAGE_QUEUE_MAX_WORKERS))
    try:
        add_MessageQueueServicer_to_server(MessageQueue(), server)
        server.add_insecure_port(MESSAGE_QUEUE_ADDRESS)
        server.start()
    except Exception as e:
        logging.error(e)

    logging.info('serving ...')
    try:
        while MESSAGE_QUEUE_RUNNING:
            time.sleep(MESSAGE_QUEUE_SLEEP_INTERVAL)
    except (InterruptedError, KeyboardInterrupt):
        server.stop(MESSAGE_QUEUE_GRACE_INTERVAL)

    logging.info('done.')


def stop():
    """
    Stop the gRPC server.
    """
    global MESSAGE_QUEUE_RUNNING
    MESSAGE_QUEUE_RUNNING = False


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    signal.signal(signal.SIGINT, lambda n, h: stop())
    signal.signal(signal.SIGTERM, lambda n, h: stop())

    serve()
