#!/usr/bin/env python3
# -*- coding: utf-8 -*-


class Cluster:

    def __init__(self, node_ip="127.0.0.1", node_port=8081, is_master_node: bool = True):
        """
            1. 启动 master cluster node socket tcp 进程
            2. 启动 选举 socket udp 进程
        """
        pass

