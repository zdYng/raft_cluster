#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import socket
import time

host = "127.0.0.1"
port = 8888
addr = (host, port)

host_2 = "127.0.0.1"
port_2 = 9999
addr_2 = (host_2, port_2)


def test_udp_2():
    udp_server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_server.bind(addr_2)
    while True:
        udp_server.sendto(b"test_udp_2 hhh send msg", addr)
        data, client_addr = udp_server.recvfrom(1024)
        print("test_udp data: {}".format(data.decode()))
        time.sleep(3)


if __name__ == '__main__':
    test_udp_2()
