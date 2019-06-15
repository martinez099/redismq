from message_queue_client import MessageQueue


mq = MessageQueue()
while True:

    mq.send_req('service-1', 'func-1', 'poaylaod')
    print(mq.recv_rsp('service-1', 'func-1'))
