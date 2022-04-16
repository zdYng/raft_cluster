#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import socket


def socket_test():
    addr = ('127.0.0.1', 8000)
    socket_node = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    socket_node.bind(addr)
    # socket_node.setblocking(True)
    socket_node.listen(5)
    n = 0
    while True:
        print(1)
        client, addr = socket_node.accept()
        print("n: {}".format(n))
        while True:
            data = client.recvfrom(1024)
            if not data:
                break
            client.send(b'123')
        client.close()
        n += 1


if __name__ == '__main__':
    # socket_test()
    a = "123"
    if a in ['ACK_RESULT_RECV_SUCCESS']:
        print("yes")
