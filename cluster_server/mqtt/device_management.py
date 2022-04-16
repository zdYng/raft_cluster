import json
import random
import string
import time

from django.conf import settings

from paho.mqtt import client as mqtt

from cluster_server.nodes.cluster_node import AssignNodePermissions


def get_client_key(prefix):
    random_key = "".join(random.sample(string.ascii_letters, 4))
    return "{0}_{1}_{2}".format(prefix, int(round(time.time() * 1000)), random_key)


MQ_BROKER_URL = "127.0.0.1"
MQ_BROKER_USER = "admin"
MQ_BROKER_PWD = "admin123"


def get_mq_client(client_id, connect=None, disconnect=None, on_message=None):
    client_id = "test-server@{}".format(client_id)
    client = mqtt.Client(client_id, protocol=mqtt.MQTTv311, clean_session=True)
    client.on_connect = connect
    client.on_disconnect = disconnect
    client.on_message = on_message
    client.username_pw_set(MQ_BROKER_USER, MQ_BROKER_PWD)
    result = client.connect(MQ_BROKER_URL, 1883, 30)
    if result != 0:
        raise ValueError("broker connect failed")
    return client


class MQTTStatisticsDevServer(object):

    def __init__(self,
                 key,
                 redis_client,
                 socket_server,
                 rabbitmq_pub_client,
                 node_is_master=False,
                 has_permission=False):
        self.node_is_master = node_is_master
        self.has_permission = has_permission
        self.topics = {}
        self.device_alarm_map = {}
        self.client = get_mq_client(get_client_key(key))
        self.client.on_log = self.on_log
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        # self.msg_queue = msg_queue
        self.uav_hb_topic = "UAV/Any/RTS/Hb"
        self.uap_hb_topic = "UAP/Any/RTS/Hb"
        # 管理在线无人机id列表
        self.online_uav_id_set = set()
        self.online_uav_id = []
        # 管理在线无人机hb_topic
        self.online_uav_hb_topic_list = []
        # 管理在线无人站id列表
        self.online_uap_id_set = set()
        self.online_uap_id = []
        # 管理在线无人站hb_topic
        self.online_uap_hb_topic_list = []

        # redis client
        self.redis_client = redis_client

        # socket client
        self.socket_server = socket_server

        # rabbitmq_pub
        self.rabbitmq_pub_client = rabbitmq_pub_client

        # DeviceManagement
        self.device_management = DeviceManagement(redis_client=self.redis_client,
                                                  rabbitmq_pub_client=self.rabbitmq_pub_client
                                                  )

    def start(self):
        self.client.loop_forever()

    def subscribe(self, topic):
        # todo 兼容老的终端固件版本
        if settings.APP_CHANNEL == "P/":
            self.topics[topic] = True
            self.client.subscribe(topic=topic, qos=0)

        topic = f"{settings.APP_CHANNEL}{topic}"
        self.topics[topic] = True
        self.client.subscribe(topic=topic, qos=0)
        print("sub topic: {}".format(topic))

    def unsubscribe(self, topic):
        self.client.unsubscribe(topic=topic)
        if topic in self.topics:
            del self.topics[topic]

    def on_log(self, client, user_data, level, buf):
        pass

    def on_connect(self, client, userdata, flags, rc):
        for topic in self.topics.keys():
            client.subscribe(topic, 0)

    @staticmethod
    def on_disconnect(client, user_data, rc):
        pass

    def on_message(self, client, user_data, msg):
        try:
            if self.node_is_master:
                self.statistics_uav_topic(msg_topic=msg.topic)
                self.statistics_uap_topic(msg_topic=msg.topic)
            if self.has_permission:
                print("topic: {}".format(msg.topic))
                print("msg.payload: {}".format(msg.payload))
        except Exception as er:
            print("uav decode mqtt msg error: {0}, topic: {1}".format(er, msg.topic))

    def decode_and_put_deque(self, msg):
        """
            队列为deque
        :param msg:
        :return:
        """
        pass

    def statistics_uav_topic(self, msg_topic):
        """
            统计uav topic
        :return:
        """
        try:
            if self.uav_hb_topic in msg_topic:
                # 属于无人机hb_topic
                if msg_topic not in self.online_uav_hb_topic_list:
                    # 新无人机上线
                    self.online_uav_hb_topic_list.append(msg_topic)
                    print(msg_topic.split("/"))
                    uav_id = int(msg_topic.split("/")[-2])
                    print("uav_id: {}".format(uav_id))
                    self.online_uav_id_set.add(uav_id)
                    print("online_uav_id_set to list: {}".format(list(self.online_uav_id_set)))
                    # 存redis
                    value = json.dumps({"all_uav_id_list": list(self.online_uav_id_set)})

                    print(value)
                    self.redis_client.hset(name="cluster:master_statistics_uav",
                                           key="all_uav_id_list",
                                           value=value)

                    self.redis_client.hset(name="cluster:master_statistics_uav",
                                           key="{}".format(uav_id),
                                           value="")  # value存被分配的node_id

                    self.device_management.allocate_device_permissions(node_is_master=self.node_is_master,
                                                                       device_type="uav",
                                                                       device_id=uav_id)

        except Exception as err:
            print("statistics_uav_topic error: {}".format(err))

    def statistics_uap_topic(self, msg_topic):
        """
            统计uap topic
        :return:
        """
        try:
            if self.uap_hb_topic in msg_topic:
                if msg_topic not in self.online_uap_hb_topic_list:
                    self.online_uap_hb_topic_list.append(msg_topic)
                    uap_id = int(msg_topic.split("/")[-2])
                    self.online_uap_id_set.add(uap_id)
                    print("online_uap_id_set to list: {}".format(list(self.online_uap_id_set)))
                    # 存redis
                    value = json.dumps({"all_uap_id_list": list(self.online_uap_id_set)})
                    print(value)
                    self.redis_client.hset(name="cluster:master_statistics_uap",
                                           key="all_uap_id_list",
                                           value=value)
                    # 查询出被分配的node_id

                    self.redis_client.hset(name="cluster:master_statistics_uap",
                                           key="{}".format(uap_id),
                                           value="")  # value存被分配的node_id
                    self.device_management.allocate_device_permissions(node_is_master=self.node_is_master,
                                                                       device_type="uap",
                                                                       device_id=uap_id)

        except Exception as err:
            print("statistics_uap_topic error: {}".format(err))


class DeviceManagement(object):
    def __init__(self, redis_client, rabbitmq_pub_client):
        self.redis_client = redis_client
        self.assign_node_permissions = AssignNodePermissions(redis_client=self.redis_client)
        self.rabbitmq_pub_client = rabbitmq_pub_client
        pass

    def allocate_device_permissions(self,
                                    node_is_master: bool = False,
                                    device_type: str = "uav",
                                    device_id: int = None):
        """
            主节点分配订阅设备权限
        :return:
        """
        data_bytes = self.redis_client.hget(name="cluster:master_statistics_{}".format(device_type),
                                            key="all_{}_id_list".format(device_type))
        if data_bytes:
            json_data = json.loads(data_bytes.decode())
            all_device_id_list = json_data.get('all_{}_id_list'.format(device_type))
            if device_id not in all_device_id_list:
                print("当前device_id {} 不在列表: {}中，无效{}".format(device_id, all_device_id_list, device_type))
                self.redis_client.hdel(name="cluster:master_statistics_{}".format(device_type),
                                       key="{}".format(device_id))
                print("redis 清除{}_id: {}".format(device_type, device_id))
                return False
            slave_node_id = self.redis_client.hget(name="cluster:master_statistics_{}".format(device_type),
                                                   key=str(device_id))
            if not slave_node_id:
                # slave_node状态评分 ， 未分配
                max_score_dict = self.assign_node_permissions.\
                    manage_devices_permission(node_is_master=node_is_master)
                if max_score_dict:
                    # rabbitMQ发消息给对应slave节点进行订阅设备操作
                    if max_score_dict.get('max_score') <= 30:
                        """
                            满负荷不进行分配 
                        """
                        print("all node load average is full ,cannot allocate device!!")
                        return False
                    max_score_dict['{}_id'.format(device_type)] = device_id
                    json_rabbitmq_pub_msg = json.dumps(max_score_dict)
                    print("json_rabbitmq_pub_msg: {}".format(json_rabbitmq_pub_msg))
                    # 更新节点信息
                    new_node_info = self.assign_node_permissions.fix_node_info(node_info=max_score_dict.get('max_score_node_info'),
                                                                               added_device_type=device_type,
                                                                               added_device_id=device_id,
                                                                               )
                    self.assign_node_permissions.update_node_info(now_node_addr=max_score_dict.get('max_score_node_address'),
                                                                  now_node_info=new_node_info,
                                                                  )
                    self.rabbitmq_pub_client.publish_msg(msg=json_rabbitmq_pub_msg,
                                                         exchange="cluster_exchange"
                                                         )
                    self.redis_client.hset(
                        name="cluster:max_score_slave_node",
                        key=max_score_dict.get('max_score_node_address'),
                        value=max_score_dict.get('max_score_node_id')
                    )

                    return True
                else:
                    print(f"device_type: {device_type} ,device_id {device_id} 没有合适的slave节点可分配")
        pass

    def new_master_node_reacquires_assignment_permission(self, node_info: dict = None, node_is_master: bool = False):
        """
            第一种场景：
            i. 前提条件：
            （1）删除失效节点-master相关信息；
            （2）触发选举服务，选举出新master节点；
            （3）当前leader节点创建新master-server ；
            （4）其他slave节点断开老master-server连接,重新与新master-server 建立新的连接操作
            ii. 分配逻辑：
            （1）新master-socket-server 服务A仅重新接管所有slave节点设备权限
            当失效节点为master节点时，重新选举出的新master节点需要掌管分配权限
        :param node_info: NodeInfo__as_dict__()
        :param node_is_master:
        :return:
        """

        if node_info is None:
            # node_info = NodeInfo.__as_dict__
            return False
        if not node_is_master:
            print("当前节点为slave节点无法操作")
            return False
        now_node_key_value = "{}:{}".format(node_info.get("node_ip"), node_info.get("node_port"))
        old_node_key_value = self.redis_client.hget(name="cluster:master_node", key="master_node_id")
        if now_node_key_value == old_node_key_value:
            print("new_master_node_reacquires_assignment_permission: master node_key has been updated")
            return True
        # 覆盖更新数据
        self.redis_client.hset(name="cluster:master_node", key="master_node_id", value=now_node_key_value)
        print("new_master_node_reacquires_assignment_permission: master node_key is updated")
        return True

    def reassign_invalid_slave_node_device_permission(self,
                                                      node_is_master: bool = False,
                                                      invalid_node_info: dict = None,
                                                      device_type: str = "uav"
                                                      ):
        """

        :param node_is_master:
        :param invalid_node_info:
        :param device_type:
        :return:
        """
        if not node_is_master:
            print("当前节点为slave节点无法重新分配失效从节点的权限")
        invalid_node_key = "{}:{}".format(invalid_node_info.get('node_ip'), invalid_node_info.get('node_port'))
        invalid_node_device_list = self.redis_client.hget(name="cluster:slave_statistics_{}".format(device_type),
                                                          key=invalid_node_key)
        if invalid_node_device_list:
            invalid_node_device_list = json.loads(invalid_node_device_list)
            print("invalid_node_device_list type: {} , value: {}".format(device_type, invalid_node_device_list))
            self.redis_client.delete("cluster:slave_statistics_{}".format(device_type), invalid_node_key)
            print("清理完成失效从节点可分配设备信息")
            return True
        else:
            # invalid_node_device_list = []
            print("失效从节点没有可分配的设备")
            return True


