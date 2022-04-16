#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from dataclasses import dataclass

from cluster_server.data_object import BasicDataObject


@dataclass
class NodeInfo(BasicDataObject):

    node_id: str = ""  # uuid
    node_ip: str = ""
    node_port: int = None
    health_status: int = None
    working_status: int = None
    load_average_status: int = None
    desc: str = "test one"


if __name__ == '__main__':
    a = NodeInfo()
    print(a.__as_dict__)
    print(type(a.__as_dict__))
