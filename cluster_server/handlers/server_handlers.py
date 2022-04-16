"""
服务端
业务处理相关处理

"""
import json
import time

from cluster_server.handlers import CommonMessageHandler


class ServerHandler:

    def __init__(self, client_socket, redis_client, address_port):
        self.common_msg_handler = CommonMessageHandler()
        self.client_socket = client_socket
        self.redis_client = redis_client
        self.address_port = address_port

        # self.online_heartbeats_key = "cluster:online_node_"
        # self.online_heartbeats_timeout = 15
        pass

    def receive_msg_handler(self, receive_msg: str):
        """

        :param receive_msg: {
                             "topic": "cluster/heartbeats",
                             "timestamp": 1648043328,
                             "msg_content": {"timestamp": 1648043328, "now_time": "2022-03-23 21:48:48", "node_id": "60fbb093-4d52-447b-9310-3d003f09076f"}
                            }
        :return:
        """
        receive_msg = json.loads(receive_msg)
        if receive_msg is None or {}:
            print("数据无效")
            return None
        if receive_msg.get('topic') == "cluster/heartbeats":
            """
                节点心跳
            """
            print("节点心跳 接收处理 ...")

            pass
        elif receive_msg.get('topic') == "cluster/health_status":
            """
                健康状态
                cluster:slave_node {""}
            """
            try:
                print("健康状态 接收处理 ...")
                slave_node_id = receive_msg.get('msg_content', {}).get('node_id')
                health_status = receive_msg.get('msg_content', {}).get('health_status')
                self.redis_client.hset(name="cluster:slave_node:health_status",
                                       key="{}".format(slave_node_id),
                                       value=health_status)
            except Exception as err:
                print("receive_msg_handler error : {}".format(err))
            pass
        elif receive_msg.get('topic') == "cluster/working_status":
            """
                工作状态
            """
            print("工作状态 接收处理 ...")
            slave_node_id = receive_msg.get('msg_content', {}).get('node_id')
            working_status = receive_msg.get('msg_content', {}).get('working_status')
            self.redis_client.hset(name="cluster:slave_node:working_status",
                                   key="{}".format(slave_node_id),
                                   value=working_status)
            pass
        elif receive_msg.get('topic') == "cluster/load_average_status":
            """
                负载状态
            """
            print("负载状态 接收处理 ...")
            slave_node_id = receive_msg.get('msg_content', {}).get('node_id')
            load_average_status = receive_msg.get('msg_content', {}).get('load_average_status')
            self.redis_client.hset(name="cluster:slave_node:load_average_status",
                                   key="{}".format(slave_node_id),
                                   value=load_average_status)
            pass
        elif receive_msg.get('topic') == "cluster/close_notify":
            """
                client端关闭通知，server端处理
            """
            print("关闭通知 接收处理 ...")
            slave_node_id = receive_msg.get('msg_content', {}).get('node_id')
            for status_type in ['health_status', 'working_status', 'load_average_status']:
                self.redis_del_status_data(status_type=status_type, node_id=slave_node_id)

    def handler_cluster_heartbeats_msg(self, receive_msg: dict):
        """
            处理心跳逻辑，刷新周期
        :param receive_msg: {

                            }
        :return:
        """

        # online_node_key = self.online_heartbeats_key + self.address_port
        # if not self.redis_client.expire(online_node_key, time=self.online_heartbeats_timeout):
        #     self.redis_client.set(key=online_node_key, value=self.address_port, ex=self.online_heartbeats_timeout)
        #     print("重置当前cluster node : {}".format(self.address_port))
        pass

    def redis_del_status_data(self, status_type, node_id):
        """
            删除client端
        :param status_type: health_status | working_status | load_average_status
        :param node_id:
        :return:
        """
        self.redis_client.hdel("cluster:slave_node:{}".format(status_type), node_id)

    def redis_del_cluster_node(self, address_port):
        """
            删除client端相关redis
        :param address_port:
        :return:
        """
        redis_data = self.redis_client.hget("cluster:all_node", address_port)
        if redis_data:
            self.redis_client.hdel("cluster:all_node", address_port)

    def send_subscribe_dev_notify_to_slave(self):
        """
            发送订阅分配权限于分节点
        :return:
        """

        pass

