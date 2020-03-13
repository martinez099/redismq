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
            channel = channel_class(channel_name, MESSAGE_QUEUE_REDIS_HOST)
            self.channels[(channel_name, channel_class)] = channel

        return channel

    def send_req(self, request, context):
        """
        Send a message.

        :param request: The client request.
        :param context: The client context.
        :return: The request ID.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        req_id = channel.send_req(request.payload)

        return SendResponse(req_id=req_id)

    def recv_rsp(self, request, context):
        """
        Receive a response.

        :param request: The client request.
        :param context: The client context.
        :return: The response payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        response = channel.recv_rsp(request.req_id, request.timeout)

        return ReceiveResponse(payload=response, req_id=request.req_id)

    def get_rsp(self, request, context):
        """
        Get a response.

        :param request: The client request.
        :param context: The client context.
        :return: The response payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        response = channel.get_rsp(request.req_id)

        return GetResponse(payload=response, req_id=request.req_id)

    def ack_rsp(self, request, context):
        """
        Acknowledge a response.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        channel = self._get_channel(request.service_name, request.func_name, Producer)
        success = channel.ack_rsp(request.req_id, request.payload)

        return AcknowledgeResponse(success=success)

    def recv_req(self, request, context):
        """
        Receive a response.

        :param request: The client request.
        :param context: The client context.
        :return: The payload of the request.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        (req_id, req) = channel.recv_req(request.timeout)

        return ReceiveResponse(payload=req, req_id=req_id)

    def get_req(self, request, context):
        """
        Get a response.

        :param request: The client request.
        :param context: The client context.
        :return: The payload of the request.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        (req_id, req) = channel.get_req()

        return GetResponse(payload=req, req_id=req_id)

    def ack_req(self, request, context):
        """
        Acknowledge a request.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        acknowleged = channel.ack_req(request.req_id)

        return AcknowledgeResponse(success=acknowleged)

    def send_rsp(self, request, context):
        """
        Send a response.

        :param request: The client request.
        :param context: The client context.
        :return: The ID of the response.
        """
        channel = self._get_channel(request.service_name, request.func_name, Consumer)
        sent = channel.send_rsp(request.req_id, request.payload)

        return SendResponse(req_id=request.req_id if sent else None)


MESSAGE_QUEUE_REDIS_HOST = os.getenv('MESSAGE_QUEUE_REDIS_HOST', 'localhost')
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
