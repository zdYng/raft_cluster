#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
import selectors
import socket
import time


class SocketNode:
    def __init__(self, server_address: str, server_port: int, timeout: int = None):
        """
            UDP
        :param server_address:
        :param server_port:
        :param timeout:
        """
        # 创建socket对象
        self.socket_node = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # 设置IP地址复用
        self.socket_node.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # self.socket_node.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # self.socket_node.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)

        if server_address and server_port:
            print("server_address: {} , server_port: {}".format(server_address, server_port))
            self.address_port = (server_address, server_port)
            self.bind(self.address_port)
        if timeout:
            self.socket_node.settimeout(timeout)

    def bind(self, address_port: tuple):
        address_port = list(address_port)
        print("address_port: {}".format(address_port))
        self.socket_node.bind((address_port[0], address_port[1]))

    # def server_connect(self, address_port):
    #     address_port = list(address_port)
    #     print("connect server: {}".format(list(address_port)))
    #     self.socket_node.connect((address_port[0], address_port[1]))

    def set_timeout(self, timeout: int):
        self.socket_node.settimeout(value=timeout)

    def send(self, msg: dict, address_port: tuple):
        """
            发送消息给指定客户端
        :param msg:
        :param address_port:
        :return:
        """
        if address_port:
            print("send msg: {}".format(msg))
            data = json.dumps(msg).encode("utf-8")
            self.socket_node.sendto(data, address_port)
            print("send ok")

    def recv(self, address_port: tuple = None, timeout: int = 10):
        """
            接收指定服务端发送的消息
        :param address_port:
        :param timeout:
        :return:
        """
        print("recv start")
        if address_port:
            print("recv value: {} , type:{}".format(list(address_port), type(list(address_port))))
            self.bind(address_port)
        print("self.address_port: {}".format(self.address_port))
        if not self.address_port:
            print("please bind to an addr")
        if timeout:
            print(f"recv timeout: {timeout}")
            self.set_timeout(timeout=timeout)
        data, addr = self.socket_node.recvfrom(65535)
        return json.loads(data), addr

    def close(self):
        self.socket_node.close()
