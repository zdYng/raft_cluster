import json
import threading

import pika

from cluster_server.mqtt.device_management import MQTTStatisticsDevServer
from cluster_server.nodes.cluster_node import AssignNodePermissions


class RabbitMQSubServer:
    def __init__(self,
                 host: str="127.0.0.1",
                 port: int=5672,
                 username: str="admin",
                 pwd: str="admin123",
                 virtual_host: str="cluster_virtual_host",
                 exchange:str = "cluster_exchange",
                 queue: str="cluster_queue",
                 redis_client=None,
                 socket_client=None,
                 is_master_node=False,
                 node_id=None,
                 node_addr=None
                 ):
        self.redis_client = redis_client
        self.socket_client = socket_client
        self.is_master_node = is_master_node
        self.username = username
        self.pwd = pwd
        self.virtual_host = virtual_host
        self.host = host
        self.port = port
        self.exchange = exchange
        self.node_id = node_id
        self.node_addr = node_addr
        self.queue = "{}:{}".format(queue, self.node_id)
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                      port=self.port,
                                                                      virtual_host=self.virtual_host,
                                                                      credentials=pika.PlainCredentials(
                                                                          username=self.username,
                                                                          password=self.pwd)))
        self.channel = self.conn.channel()
        self.channel.exchange_declare(exchange=self.exchange, durable=True, exchange_type="fanout")
        # self.channel.queue_declare(queue=queue, durable=True, exclusive=True)
        # self.channel.queue_bind(exchange=self.exchange, queue=self.queue)
        self.assign_node_permissions = AssignNodePermissions(redis_client=self.redis_client)
        pass

    def new_queue(self, queue):
        """
            1
        :param queue:
        :return:
        """
        self.channel.queue_declare(queue=queue, durable=True, exclusive=True)

    def bind_queue_exchange(self, queue, exchange, routing_key=""):
        """
            2
        :param queue:
        :param exchange:
        :param routing_key:
        :return:
        """
        self.channel.queue_bind(exchange=exchange, queue=queue, routing_key=routing_key)

    def publish_msg(self, msg, exchange="cluster_exchange", ):
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=self.queue,
                                   body=msg)
        pass

    def subscribe_msg(self, exchange="cluster_exchange"):
        self.new_queue(queue=self.queue)
        self.channel.queue_bind(exchange=exchange,
                                queue=self.queue
                                )
        self.channel.basic_consume(on_message_callback=self.on_msg_callback, queue=self.queue, auto_ack=False)
        self.channel.start_consuming()

    def on_msg_callback(self, ch, method, properties, body):
        try:
            print("sub")
            print(f"received {body.decode()} , {type(json.loads(body.decode()))}")
            msg_dict = json.loads(body.decode())

            # threading.Thread(target=self.start_mqtt_subscribe, args=(msg_dict,)).start()
            if msg_dict.get('max_score_node_id') == self.node_id:

                if msg_dict.get('uav_id'):
                    self.new_thread_mqtt_sub(func=self.start_mqtt_uav_subscribe, args=(msg_dict,), msg_dict=msg_dict)
                    # 更新当前节点状态信息
                    now_node_addr = self.node_addr
                    self.redis_client.hset(name="cluster:master_statistics_uav",
                                           key=msg_dict.get('uav_id'),
                                           value=now_node_addr)
                    slave_node_uav_list = self.redis_client.hget(name="cluster:slave_statistics_uav",
                                                                 key=now_node_addr)
                    if not slave_node_uav_list:
                        slave_node_uav_list = []
                    else:
                        slave_node_uav_list = json.loads(slave_node_uav_list)

                    self.redis_client.hset(name="cluster:slave_statistics_uav",
                                           key=now_node_addr,
                                           value=json.dumps(slave_node_uav_list)
                                           )
                    # now_node_info = {}
                    # self.assign_node_permissions.update_node_info(now_node_addr=now_node_addr, now_node_info=now_node_info)
                elif msg_dict.get('uap_id'):
                    self.new_thread_mqtt_sub(func=self.start_mqtt_uap_subscribe, args=(msg_dict,), msg_dict=msg_dict)
                    # 更新当前节点状态信息
                    now_node_addr = self.node_addr
                    self.redis_client.hset(name="cluster:master_statistics_uap",
                                           key=msg_dict.get('uap_id'),
                                           value=now_node_addr)
                    slave_node_uap_list = self.redis_client.hget(name="cluster:slave_statistics_uap",
                                                                 key=now_node_addr)
                    if not slave_node_uap_list:
                        slave_node_uap_list = []
                    else:
                        slave_node_uap_list = json.loads(slave_node_uap_list)

                    self.redis_client.hset(name="cluster:slave_statistics_uap",
                                           key=now_node_addr,
                                           value=json.dumps(slave_node_uap_list)
                                           )
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f"error: {e}")
        # print(f"queue num: {self.channel.queue_declare(self.queue).method.message_count}")

    def new_thread_mqtt_sub(self, func, args, msg_dict):
        if msg_dict.get('max_score_node_id') == self.node_id:
            threading.Thread(target=func, args=args).start()

    def start_mqtt_uav_subscribe(self, msg_dict):
        print("on_msg_callback start_mqtt_subscribe ...")

        mqtt_statistics_dev_server = MQTTStatisticsDevServer(key="slave_node_statistics",
                                                             redis_client=self.redis_client,
                                                             socket_server=self.socket_client,
                                                             rabbitmq_pub_client=None,
                                                             node_is_master=self.is_master_node,
                                                             has_permission=True
                                                             )

        topic = "UAV/Any/RTS/+/{}/".format(msg_dict.get('uav_id'))
        print("topic: {}".format(topic))
        mqtt_statistics_dev_server.subscribe(topic=topic)
        mqtt_statistics_dev_server.start()

        pass

    def start_mqtt_uap_subscribe(self, msg_dict):
        print("on_msg_callback start_mqtt_uap_subscribe ...")

        mqtt_statistics_dev_server = MQTTStatisticsDevServer(key="slave_node_statistics",
                                                             redis_client=self.redis_client,
                                                             socket_server=self.socket_client,
                                                             rabbitmq_pub_client=None,
                                                             node_is_master=self.is_master_node,
                                                             has_permission=True
                                                             )

        topic = "UAP/Any/RTS/+/{}/".format(msg_dict.get('uap_id'))
        print("topic: {}".format(topic))
        mqtt_statistics_dev_server.subscribe(topic=topic)
        mqtt_statistics_dev_server.start()

        pass

