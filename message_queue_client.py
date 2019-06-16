import uuid

import grpc

from message_queue_pb2 import SendRequest, ReceiveRequest, GetRequest, AcknowledgeRequest
from message_queue_pb2_grpc import MessageQueueStub


MESSAGE_QUEUE_ADDRESS = 'message-queue:50051'


class MessageQueue(object):
    """
    Message Queue class.
    """

    def __init__(self):
        self.channel = grpc.insecure_channel(MESSAGE_QUEUE_ADDRESS)
        self.stub = MessageQueueStub(self.channel)
        self.subscribers = {}

    def __del__(self):
        self.channel.close()

    def send_req(self, service_name, func_name, payload, req_id=None):
        """
        Send a request.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :param payload: The request payload.
        :param req_id: The ID of the request, may be None.
        :return: The ID of the sent request.
        """
        response = self.stub.send_req(
            SendRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=req_id or str(uuid.uuid4()),
                payload=payload,
            ))

        return response.req_id

    def recv_rsp(self, service_name, func_name, req_id):
        """
        Receive a response.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :param req_id: The ID of the request, may be None.
        :return: The payload of the response.
        """
        response = self.stub.recv_rsp(
            ReceiveRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=req_id,
            ))

        return response.payload

    def get_rsp(self, service_name, func_name, req_id):
        """
        Get a response.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :param req_id: The ID of the request, may be None.
        :return: The payload of the response.
        """
        response = self.stub.get_rsp(
            GetRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=req_id,
            ))

        return response.payload

    def ack_rsp(self, service_name, func_name, req_id, payload):
        """
        Acknowledge a response.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :param req_id: The ID of the request.
        :param payload: The payload of the response.
        :return: Success.
        """
        response = self.stub.ack_rsp(
            AcknowledgeRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=req_id,
                payload=payload,
            ))

        return response.success

    def recv_req(self, service_name, func_name):
        """
        Receive a request.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :return: The payload of the request.
        """
        request = self.stub.recv_req(
            ReceiveRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=None,
            ))

        return request.payload

    def get_req(self, service_name, func_name):
        """
        Get a request.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :return: The payload of the response.
        """
        request = self.stub.get_req(
            GetRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=None,
            ))

        return request.payload

    def ack_req(self, service_name, func_name, req_id):
        """
        Acknowledge a request.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :param req_id: The ID of the request.
        :return: The payload of the response.
        """
        request = self.stub.ack_req(
            AcknowledgeRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=req_id,
                payload=None,
            ))

        return request.success

    def send_rsp(self, service_name, func_name, req_id, payload):
        """
        Send a response.

        :param service_name: The remote service to call.
        :param func_name: The remote function to call.
        :param req_id: The ID of the request.
        :param payload: The payload of the response.
        :return: Success.
        """
        response = self.stub.send_rsp(
            SendRequest(
                service_name=service_name,
                func_name=func_name,
                req_id=req_id,
                payload=payload,
            ))

        return response.req_id
