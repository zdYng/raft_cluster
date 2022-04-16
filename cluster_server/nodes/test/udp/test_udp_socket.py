#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import socket

host = "127.0.0.1"
port = 10002
addr = (host, port)


def test_udp():
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.bind(addr)
    while True:
        data, client_addr = udp_server.recvfrom(1024)
        print("test_udp_2 data: {}".format(data.decode()))

        udp_server.sendto(b"test_udp hhh send msg", client_addr)


if __name__ == '__main__':
    test_udp()
