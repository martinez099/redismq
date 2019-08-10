import threading

from message_queue_client import MessageQueue

MQ = MessageQueue()


def sender(mq, service_name, func_name):

    while True:

        # send a request
        req_id = mq.send_req(service_name, func_name, 'poaylaod')

        # receive the response
        rsp = mq.recv_rsp(service_name, func_name, req_id, 1)

        # acknowledge the response
        mq.ack_rsp(service_name, func_name, req_id, rsp)

        print(rsp)


def receiver(mq, service_name, func_name):

    while True:

        # receive a request
        req_id, req_payload = mq.recv_req(service_name, func_name, 1)

        # do some processing
        rsp = req_payload + '_processed by {}.{}'.format(service_name, func_name)

        # send a response
        mq.send_rsp(service_name, func_name, req_id, rsp)

        # acknowledge the request
        mq.ack_req(service_name, func_name, req_id)


RECEIVERS = SENDERS = 9

threads = [threading.Thread(
                        target=sender,
                        name='Sender(service-1.func_{})'.format(i),
                        args=(MQ, 'service-1', 'func_{}'.format(i))) for i in range(SENDERS)] + \
          [threading.Thread(
                        target=receiver,
                        name='Receiver(service-1.func_{})'.format(i),
                        args=(MQ, 'service-1', 'func_{}'.format(i))) for i in range(RECEIVERS)]

[t.start() for t in threads]
[t.join() for t in threads]
