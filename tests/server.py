from redismq import Consumer

import json

import gevent

from gevent import monkey
monkey.patch_all()


channel = Consumer('channel1')


def handle_req(_id, _value):

    # process the message
    values = json.loads(_value)

    # send response
    channel.send_rsp(_id, json.dumps({
        "cnt": str(values['cnt']),
        "msg": values['data'] + '_processed'
    }))

    # acknowledge message
    channel.ack_req(_id)

    return True


print('serving ...')
while True:

    # receive the message
    req_id, req = channel.recv_req()

    # handle the message
    if not gevent.spawn(handle_req, req_id, req):
        break
