from message_queue_client import MessageQueue


mq = MessageQueue()
while True:

    SERVICE_NAME = 'service-1'
    FUNC_NAME = 'func-1'

    # send a request
    req_id = mq.send_req(SERVICE_NAME, FUNC_NAME, 'poaylaod')

    # receive the same request
    req = mq.recv_req(SERVICE_NAME, FUNC_NAME, req_id)

    # do some processing
    rsp = req + '_processed'

    # acknowledge the request
    mq.ack_req(SERVICE_NAME, FUNC_NAME, req_id)

    # send a response
    mq.send_rsp(SERVICE_NAME, FUNC_NAME, req_id, rsp)

    # receive a response
    rsp = mq.recv_rsp(SERVICE_NAME, FUNC_NAME, req_id)

    # acknowledge the response
    mq.ack_rsp(SERVICE_NAME, FUNC_NAME, req_id, rsp)

    print(rsp)
