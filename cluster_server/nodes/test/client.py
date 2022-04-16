#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import time

import redis

from cluster_server.servers.socket_node import SocketNode


def test_client_send_msg():
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    socket_node = SocketNode(server_address="127.0.0.1",
                             server_port=6666)
    addr = redis_client.hget(name="cluster:master_node", key="master_node_id").decode()
    print(addr)
    ip, port = addr.split(":")
    while True:
        data = {"type": "client_append_entries", "timestamp": int(time.time())}
        socket_node.send(data, (ip, int(port)))
        time.sleep(10)
        print("send ok")
        try:
            recv, _ = socket_node.recv(timeout=5)
            print("recv success: {}".format(recv))
        except Exception as err:
            print("error: {}".format(err))


if __name__ == '__main__':
    test_client_send_msg()