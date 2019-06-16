import logging
import time
from concurrent import futures

import grpc
from redis import StrictRedis

from message_queue_pb2 import SendResponse, ReceiveResponse, GetResponse, AcknowledgeResponse
from message_queue_pb2_grpc import MessageQueueServicer, add_MessageQueueServicer_to_server

from redismq.channel import Sender, Receiver


class MessageQueue(MessageQueueServicer):
    """
    Message Queue class.
    """

    def __init__(self):
        self.redis = StrictRedis(decode_responses=True, host='localhost')
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
            channel = channel_class(channel_name, self.redis)
            self.channels[(channel_name, channel_class)] = channel

        return channel

    def send_req(self, request, context):
        """
        Send a message.

        :param request: The client request.
        :param context: The client context.
        :return: The request ID.
        """
        channel = self._get_channel(request.service_name, request.func_name, Sender)
        req_id = channel.send_req(request.payload)

        return SendResponse(req_id=req_id)

    def recv_rsp(self, request, context):
        """
        Receive a response.

        :param request: The client request.
        :param context: The client context.
        :return: The response payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Sender)
        response = channel.recv_rsp(request.req_id)

        return ReceiveResponse(payload=response, req_id=request.req_id)

    def get_rsp(self, request, context):
        """
        Get a response.

        :param request: The client request.
        :param context: The client context.
        :return: The response payload.
        """
        channel = self._get_channel(request.service_name, request.func_name, Sender)
        response = channel.get_rsp(request.req_id)

        return GetResponse(payload=response, req_id=request.req_id)

    def ack_rsp(self, request, context):
        """
        Acknowledge a response.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        channel = self._get_channel(request.service_name, request.func_name, Sender)
        success = channel.ack_rsp(request.req_id, request.payload)

        return AcknowledgeResponse(success=success)

    def recv_req(self, request, context):
        """
        Receive a response.

        :param request: The client request.
        :param context: The client context.
        :return: The payload of the request.
        """
        channel = self._get_channel(request.service_name, request.func_name, Receiver)
        (req_id, req) = channel.recv_req()

        return ReceiveResponse(payload=req, req_id=req_id)

    def get_req(self, request, context):
        """
        Get a response.

        :param request: The client request.
        :param context: The client context.
        :return: The payload of the request.
        """
        channel = self._get_channel(request.service_name, request.func_name, Receiver)
        (req_id, req) = channel.get_req()

        return GetResponse(payload=req, req_id=req_id)

    def ack_req(self, request, context):
        """
        Acknowledge a request.

        :param request: The client request.
        :param context: The client context.
        :return: Success.
        """
        channel = self._get_channel(request.service_name, request.func_name, Receiver)
        acknowleged = channel.ack_req(request.req_id)

        return AcknowledgeResponse(success=acknowleged)

    def send_rsp(self, request, context):
        """
        Send a response.

        :param request: The client request.
        :param context: The client context.
        :return: The ID of the response.
        """
        channel = self._get_channel(request.service_name, request.func_name, Receiver)
        sent = channel.send_rsp(request.req_id, request.payload)

        return SendResponse(req_id=request.req_id if sent else None)


EVENT_STORE_ADDRESS = '[::]:50051'
EVENT_STORE_THREADS = 10
EVENT_STORE_SLEEP_INTERVAL = 60 * 60 * 24
EVENT_STORE_GRACE_INTERVAL = 0


def serve():
    """
    Run the gRPC server.
    """
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=EVENT_STORE_THREADS))
    try:
        add_MessageQueueServicer_to_server(MessageQueue(), server)
        server.add_insecure_port(EVENT_STORE_ADDRESS)
        server.start()
    except Exception as e:
        logging.log(logging.ERROR, str(e))

    logging.log(logging.INFO, 'serving ...')
    try:
        while True:
            time.sleep(EVENT_STORE_SLEEP_INTERVAL)
    except KeyboardInterrupt:
        server.stop(EVENT_STORE_GRACE_INTERVAL)

    logging.log(logging.INFO, 'done')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    serve()
