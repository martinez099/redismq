from redismq import Sender

import json
import time

import redis


r = redis.StrictRedis(decode_responses=True)
channel = Sender('channel1', r)


def handle_rsp(_ctx, _msg):
    rsp_id = _msg['data']

    # try to get response
    rsp = channel.get_rsp(rsp_id)
    if rsp:

        # process response
        print('{}: {}'.format(_ctx, rsp))

        # acknowledge response
        channel.ack_rsp(rsp_id, rsp)


def recv_rsp(_id, _cnt):

    # waiting for response
    rsp = channel.recv_rsp(_id)

    # process response
    print(rsp)

    # acknowledge response
    channel.ack_rsp(_id, rsp)


start = time.time()

#hndl = channel.set_rsp_handler(functools.partial(handle_rsp, {}))
for i in range(0, 100):
    req_id = channel.send_req(json.dumps({"cnt": i, "data": 'a_request'}))
    recv_rsp(req_id, i)

end = time.time()
duration = end - start
print(duration)

print('done')
