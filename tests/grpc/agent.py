import threading

from message_queue_client import Receivers, send_message


def sender(service_name, func_name):

    while True:

        print(send_message(service_name, func_name, 'poaylaod'))


def test_func(req):

    return req + "_processed"


threading.Thread(target=sender, args=('test-service', 'test_func')).start()

rs = Receivers('test-service', [test_func]).start().wait()
