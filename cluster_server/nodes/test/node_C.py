#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import redis

from cluster_server.nodes.raft_elect_node import RaftElectNode

if __name__ == '__main__':

    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    node = RaftElectNode(redis_client=redis_client,
                         node_address="127.0.0.1",
                         node_port=10003,
                         now_node_id="node-C",
                         other_node_info={"node-B": ("127.0.0.1", 10002),
                                          "node-A": ("127.0.0.1", 10001),
                                          "node-D": ("127.0.0.1", 10004),
                                          "node-E": ("127.0.0.1", 10005),
                                          }
                         )
    # node.socket_node.server_connect(("127.0.0.1", 9999))
    node.run()