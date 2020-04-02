from redismq import Producer

import json
import functools
import time


channel = Producer('channel1')


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
    rsp = channel.recv_rsp(_id, 1)

    # process response
    print(rsp)

    # acknowledge response
    channel.ack_rsp(_id, rsp)


start = time.time()

channel.set_rsp_handler(functools.partial(handle_rsp, {}))
for i in range(0, 100):
    msg_id = channel.send_msg(json.dumps({"cnt": i, "data": 'a_request'}))
    #recv_rsp(msg_id, i)

end = time.time()
duration = end - start
print(duration)

print('done')
