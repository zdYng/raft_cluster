"""
客户端
业务处理相关处理
"""
import datetime
import json
import time

from cluster_server.handlers import CommonMessageHandler


class ClientHandlers:
    def __init__(self, client_socket, redis_client, node_id, node_address_port):
        self.common_msg_handler = CommonMessageHandler()
        self.client_socket = client_socket
        self.redis_client = redis_client
        self.node_id = node_id
        self.node_address_port = node_address_port

        self.online_heartbeats_key = "cluster:online_node_{}".format(self.node_address_port)
        self.online_heartbeats_timeout = 15
        pass

    def send_heartbeats_msg(self):
        try:
            msg_content_data = {"timestamp": int(time.time()),
                                "now_time": str(datetime.datetime.now())[:19],
                                "node_id": self.node_id,
                                "node_address_port": self.node_address_port
                                }
            msg_data = {
                "topic": self.common_msg_handler.heartbeats_topic,
                "timestamp": int(time.time()),
                "msg_content": msg_content_data
            }
            json_data = json.dumps(msg_data)
            self.client_socket.sendall(bytes(json_data, encoding="utf-8"))
            print("send_heartbeats_msg json_data: {}".format(json_data))
            return True
        except Exception as err:
            print("send_heartbeats_msg error: {}".format(err))
            return False

    def client_heartbeats_msg_receive(self):
        try:
            # server端返回心跳状态消息ack
            server_data = self.client_socket.recv(1024)
            print("server send client_heartbeats_msg_receive ： ", str(server_data, encoding='utf-8'))
            # 刷新redis在线周期
            self.handler_cluster_heartbeats_msg()
        except Exception as err:
            print("client_heartbeats_msg_receive error: {}".format(err))

    def handler_cluster_heartbeats_msg(self):
        """
            处理心跳逻辑，刷新周期
        :return:
        """

        if not self.redis_client.expire(self.online_heartbeats_key, time=self.online_heartbeats_timeout):
            self.redis_client.set(key=self.online_heartbeats_key,
                                  value=self.node_address_port,
                                  ex=self.online_heartbeats_timeout)
            print("重置当前cluster node : {} 在线周期: {}".format(self.node_address_port, self.online_heartbeats_timeout))
        pass

    def heartbeats_msg_handler(self):
        self.send_heartbeats_msg()
        self.client_heartbeats_msg_receive()

    def send_status_msg(self, msg_topic_type: str, status_msg: dict):
        """

        :param msg_topic_type:
        :param status_msg: {"status": ""}
        :return:
        """
        if not status_msg:
            print("status_msg cannot be None")
            return False
        if msg_topic_type == "health":
            msg_topic = self.common_msg_handler.health_status_topic
        elif msg_topic_type == "working":
            msg_topic = self.common_msg_handler.working_status_topic
        elif msg_topic_type == "load_average":
            msg_topic = self.common_msg_handler.load_average_status_topic
        else:
            print("send_msg topic {} error".format(msg_topic_type))
            return False
        new_status_msg = {
            "topic": msg_topic,
            "timestamp": int(time.time()),
            "msg_content": status_msg
        }
        json_data = json.dumps(new_status_msg)
        self.client_socket.sendall(bytes(json_data, encoding="utf-8"))
        print("send_status_msg json_data: {}".format(json_data))
        return True

    def health_status_msg_handler(self, health_status):
        status_msg = {
            "health_status": health_status,
            "node_id": self.node_id
        }
        self.send_status_msg(msg_topic_type="health", status_msg=status_msg)
        self.client_health_status_msg_receive()

    def client_health_status_msg_receive(self):
        try:
            # server端返回健康状态消息ack
            server_data = self.client_socket.recv(1024)
            print("server send health_status_msg_receive ： ", str(server_data, encoding='utf-8'))
        except Exception as err:
            print("client_recv error: {}".format(err))

    def working_status_msg_handler(self, working_status):
        status_msg = {
           "working_status": working_status,
           "node_id": self.node_id
        }
        self.send_status_msg(msg_topic_type="working", status_msg=status_msg)
        self.client_working_status_msg_receive()

    def client_working_status_msg_receive(self):
        try:
            # server端返回working状态消息ack
            server_data = self.client_socket.recv(1024)
            print("server send working_status_msg_receive ： ", str(server_data, encoding='utf-8'))

        except Exception as err:
            print("client_recv error: {}".format(err))

    def load_average_msg_handler(self, load_average_status):
        status_msg = {
            "load_average_status": load_average_status,
            "node_id": self.node_id
        }
        self.send_status_msg(msg_topic_type="load_average", status_msg=status_msg)
        self.client_load_average_msg_receive()

    def client_load_average_msg_receive(self):
        try:
            # server端返回负载状态消息ack
            server_data = self.client_socket.recv(1024)
            print("server send load_average_msg_receive ： ", str(server_data, encoding='utf-8'))

        except Exception as err:
            print("client_recv error: {}".format(err))
