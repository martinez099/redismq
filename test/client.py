from channel import Client

import redis
import json
import time


r = redis.StrictRedis(decode_responses=True)
channel = Client('channel1', r)


def handle_rsp(_msg):
    rsp_id = _msg['data']
    rsp = channel.get_rsp(rsp_id)
    if rsp:

        # process response
        print(rsp)

        channel.ack_rsp(rsp_id, rsp)


def recv_rsp(_id, _cnt):

    # waiting for response
    rsp = channel.recv_rsp(_id)

    values = json.loads(rsp)

    # check values
    assert int(values['cnt']) == _cnt

    # process response
    print(values)

    channel.ack_rsp(_id, rsp)


def send_req(_cnt):

    # send request
    return channel.send_req(json.dumps({"cnt": _cnt,
                                        "data": 'a_request'}))


start = time.time()
hndl = channel.set_rsp_handler(handle_rsp)

for i in range(0, 1000):
    req_id = send_req(i)
    #recv_rsp(req_id, i)

end = time.time()
duration = end - start
print(duration)

print('done')
