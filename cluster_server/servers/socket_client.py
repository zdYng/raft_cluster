"""

"""
import datetime
import json
import socket
import time
import uuid
import os
import django

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "antwork_backend.settings")
django.setup(set_prefix=False)
from cluster_server.enums.status_enums import NodeHealthStatusEnum, NodeWorkingStatusEnum, NodeLoadAverageStatusEnum
from cluster_server.handlers.client_handlers import ClientHandlers


class SocketClient:
    """
        TCP Client
    """
    redis_client: object

    def __init__(self, server_address, server_port, client_address, client_port, node_id=None):
        self.server_address = server_address
        self.server_port = server_port
        self.client_address = client_address
        self.client_port = client_port

        # address:port
        self.client_address_port = f"{self.client_address}:{self.client_port}"

        self.retry_times = 0
        if not node_id:
            self.node_id = str(uuid.uuid4())

        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        # self.client_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 20)
        self.client_socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 1)
        # self.client_socket.setsockopt()

        self.client_handlers = ClientHandlers(client_socket=self.client_socket,
                                              redis_client=self.redis_client,
                                              node_id=self.node_id,
                                              node_address_port=self.client_address_port
                                              )

        # 健康状态
        self.old_health_status = None  # 上一个状态
        self.now_health_status = NodeHealthStatusEnum.__ONLINE_HEALTH_STATUS__  # 当前健康状态，默认为离线

        # 工作状态
        self.old_working_status = None  # 上一个状态
        self.now_working_status = NodeWorkingStatusEnum.__IDLE_WORKING_STATUS__  # 空闲工作状态

        # 负载状态
        self.old_load_average_status = None  # 上一个状态
        self.now_load_average_status = NodeLoadAverageStatusEnum.__ZERO_LOAD_AVERAGE_STATUS__  # 0 负载状态

        pass

    def start_client_connect_server(self):
        try:
            print("start client....")
            # 指定client ip:port
            self.client_socket.bind((self.client_address, self.client_port))
            self.client_socket.connect((self.server_address, self.server_port))
            while True:
                # self.send_msg(msg_data=msg_data)
                # self.client_recv()

                self.client_handlers.heartbeats_msg_handler()
                time.sleep(0.1)
                self.send_status_msg()
                time.sleep(10)

        except Exception as err:
            self.close_client()
            print("start_client_connect_server error: {}".format(err))

    # def send_heartbeats_msg(self):
    #     try:
    #         msg_data = {"timestamp": int(time.time()),
    #                     "now_time": str(datetime.datetime.now())[:19],
    #                     "node_id": str(self.node_id)}
    #         json_data = json.dumps(msg_data)
    #         print("send heartbeats_msg data: {}".format(json_data))
    #         self.client_socket.sendall(bytes(json_data, encoding="utf-8"))
    #         self.client_recv()
    #     except Exception as err:
    #         print("send heartbeats_msg error: {}".format(err))
    #
    # def client_recv(self):
    #     try:
    #         server_data = self.client_socket.recv(1024)
    #         print("服务器发来信息： ", str(server_data, encoding='utf-8'))
    #     except Exception as err:
    #         print("client_recv error: {}".format(err))

    def reconnect(self):
        """

        :return:
        """
        try:
            if self.retry_times < 3:
                self.start_client_connect_server()
            else:
                self.close_client()
        except Exception as err:
            self.close_client()
            print("reconnect error: {}".format(err))

    def close_client(self):

        close_notify_msg = {
            "topic": "cluster/close_notify",
            "timestamp": int(time.time()),
            "msg_content": {
                "node_id": self.node_id
            }
        }
        json_data = json.dumps(close_notify_msg)
        self.client_socket.sendall(bytes(json_data, encoding="utf-8"))

        self.client_socket.close()
        print("close client")

        exit()

    def send_status_msg(self):
        if self.now_health_status != self.old_health_status:
            self.client_handlers.health_status_msg_handler(health_status=self.now_health_status)
            self.old_health_status = self.now_health_status
            time.sleep(0.1)
            # working status
        if self.now_working_status != self.old_working_status:
            self.client_handlers.working_status_msg_handler(working_status=self.now_working_status)
            self.old_working_status = self.now_working_status
            time.sleep(0.1)
            # load average status
        if self.now_load_average_status != self.old_load_average_status:
            self.client_handlers.load_average_msg_handler(load_average_status=self.now_load_average_status)
            self.old_load_average_status = self.now_load_average_status

    # def elect_master_node(self, redis_client, now_node_info):
    #     """
    #         todo
    #     :param redis_client:
    #     :param now_node_info:
    #     :return:
    #     """
    #     elect_mode = ElectMode(redis_client=redis_client)
    #     max_node = elect_mode.elect_master_node(now_node_info)

    def clear_redis_cluster_info(self, redis_client):
        """
            清理cluster:all_node：
            （1）离线时
            （2）当前节点选举为master
        :param redis_client:
        :return:
        """
        self.redis_client = redis_client

        # cluster:all_node
        key = "{}:{}".format(self.server_address,self.server_port)
        master_node_data = self.redis_client.hget(name="cluster:all_node", key=key)
        if master_node_data:
            self.redis_client.hdel("cluster:all_node", key)


if __name__ == '__main__':
    socket_client = SocketClient(server_address="127.0.0.1", server_port=8080,
                                 node_id=None, client_address="127.0.0.1", client_port=7070)
    socket_client.start_client_connect_server()



