"""
cluster
master  ：
（1）统一管理各节点订阅的终端设备消息接收
（2）同步各节点终端设备订阅信息
slave ：
（1）记录管理当前节点下的订阅的终端设备信息
（2）同步给master主节点当前节点终端订阅消息
"""
import json
import threading
import time
import uuid
import redis

from cluster_server.enums.status_enums import NodeHealthStatusEnum, NodeWorkingStatusEnum, \
    NodeLoadAverageStatusEnum
from cluster_server.mqtt.device_management import MQTTStatisticsDevServer
from cluster_server.rabbitmq_server.pub_server import RabbitMQPubServer
from cluster_server.rabbitmq_server.sub_server import RabbitMQSubServer
from cluster_server.servers.socket_client import SocketClient
from cluster_server.servers.socket_server import SocketServer


class ClusterNodeServer:

    def __init__(self, node_ip="127.0.0.1", node_port=8081, is_master_node: bool=True):
        """
            {
                "node_id": "3ee2d347-d315-4cb0-a689-2cd286ff5a49",
                "node_ip": "127.0.0.1",
                "node_port": 9090,
                "health_status": 1,
                "working_status": 0,
                "load_average_status": 0,
                "desc": "test one",
                "subscribe_devices": {"subscribe_uav_id_list": [], "subscribe_uap_id_list": []}
            }
        :param node_ip:
        :param node_port:
        """
        # 节点ip
        self.node_ip = node_ip
        # 节点端口
        self.node_port = node_port
        # 健康状态
        self.health_status = NodeHealthStatusEnum.__ONLINE_HEALTH_STATUS__  # 当前健康状态，默认为离线

        # 工作状态
        self.working_status = NodeWorkingStatusEnum.__IDLE_WORKING_STATUS__  # 空闲工作状态

        # 负载状态
        self.load_average_status = NodeLoadAverageStatusEnum.__ZERO_LOAD_AVERAGE_STATUS__  # 0 负载状态

        # 节点相关信息
        # 所有节点id列表 (去重)  set()
        self.all_node_list = list()  # or None
        self.node_id = str(uuid.uuid4())
        self.now_node = {
            "node_id": self.node_id,  # uuid
            "node_ip": self.node_ip,
            "node_port": self.node_port,
            "health_status": self.health_status,
            "working_status": self.working_status,
            "load_average_status": self.load_average_status,
            "desc": "test one",
        }
        self.now_node_key = "{}:{}".format(self.node_ip,
                                           self.node_port)
        self.is_master_node = is_master_node
        self.slave_list = []
        self.health_check = {
            "status": "",
        }

        # 当前节点分配的消息订阅权限
        self.sub_uav_id_list = []  # 订阅的无人机id
        self.sub_uap_id_list = []  # 订阅的无人站id
        self.subscribe_devices = {
            "subscribe_uav_id_list": self.sub_uav_id_list,
            "subscribe_uap_id_list": self.sub_uap_id_list
        }
        self.now_node['subscribe_devices'] = self.subscribe_devices
        self.mqtt_statistics_dev_server = None
        self.subscribe_topic_list = []
        # redis client
        self.redis_client = RedisCommand(host="127.0.0.1",
                                         db=22,
                                         port=6379,
                                         password="asdFGHJKL321",
                                         ).redis_client
        # self.create_redis_client()
        print("socket server init")
        # socket server
        self.socket_server = None
        self.socket_server_thread = None
        # socket client
        self.socket_client = None
        self.socket_client_thread = None

        # rabbitMQ pub
        self.rabbitmq_pub_client = None

        # rabbitMQ sub
        self.rabbitmq_sub_client = None

        # 开启rabbitMQ pub/sub 服务
        self.create_rabbitmq_pub_client()
        self.create_rabbitmq_sub_client()

        print("__init__ ok")

    def start_service(self):
        """
            启动服务
            1. 查询redis数据：
                （1）判断master节点是否存在
                （2）生成节点信息
                （3）redis同步节点信息于集群
        :return:
        """
        if self.is_master_node:
            print("清空redis旧数据")
            keys = self.redis_client.keys("cluster:*")
            for key in keys:
                self.redis_client.delete(key)
        print("starting...")

        if self.redis_client is None:
            print("redis connection is exception ! ")
            raise Exception("redis connection is exception ! ")

        add_node_result = self.add_node_in_clusters()
        if add_node_result is False:
            exit()
            return False

        # cluster_node_list = self.search_all_node_list()

        # 判断当前节点类型
        if self.is_master_node:
            # master节点启动server服务
            print("启动当前节点（master）server服务")
            self.start_socket_server()
            print("started")
        else:
            # slave节点启动client服务
            print("启动当前节点（slave）client服务")
            self.start_socket_client()

        # 开启mqtt服务
        # mqtt 统计在线设备
        print("__subscribe_msg__ start ...")
        self.__subscribe_msg__(is_master_node=self.is_master_node)

        # 选举机制，重新开启服务

        # 当前节点属于最高分节点，则
        # （1）开启master服务，
        # （2）修改master节点相关数据，
        # （3）并删除slave节点相关数据
        # （4）通知其他slave节点重启socket-client通信

        # 接收到新master节点重启socket-client通知
        # print("start_service globals: {}".format(globals()))
        pass

    def start_socket_server(self):
        """
            开启socket-server 进程服务
        :return:
        """
        try:
            self.socket_server = SocketServer(server_address=self.node_ip,
                                              server_port=self.node_port,
                                              redis_client=self.redis_client)
            self.socket_server_thread = threading.Thread(target=self.socket_server.start_server_forever)
            # self.socket_server_thread = Process(target=self.socket_server.start_server_forever)
            self.socket_server_thread.start()
            print("start_socket_server started ...")
        except Exception as err:
            print("start_socket_server error: {}".format(err))

    def start_socket_client(self):
        """
            开启socket-client 进程服务
        :return:
        """
        master_node_ip, master_node_port = self.search_master_node()
        self.socket_client = SocketClient(server_address=master_node_ip, server_port=master_node_port,
                                          client_address=self.node_ip, client_port=self.node_port)
        self.socket_client_thread = threading.Thread(target=self.socket_client.start_client_connect_server)
        self.socket_client_thread.start()

    def __subscribe_msg__(self, is_master_node=True):
        if is_master_node:
            threading.Thread(target=self.__master_subscribe_msg__).start()
            return True
        else:
            threading.Thread(target=self.__slave_subscribe_msg__).start()
            return True

    def __master_subscribe_msg__(self):
        """
            主节点订阅逻辑
        :return:
        """
        print("__master_subscribe_msg__ start")
        if self.rabbitmq_pub_client:
            self.mqtt_statistics_dev_server = MQTTStatisticsDevServer(key="master_node_statistics",
                                                                      redis_client=self.redis_client,
                                                                      socket_server=self.socket_client,
                                                                      rabbitmq_pub_client=self.rabbitmq_pub_client,
                                                                      node_is_master=self.is_master_node
                                                                      )
            self.subscribe_topic_list = ["UAV/Any/RTS/Hb/#", "UAP/Any/RTS/Hb/#"]
            for topic in self.subscribe_topic_list:
                self.mqtt_statistics_dev_server.subscribe(topic=topic)
            self.mqtt_statistics_dev_server.start()
        else:
            print("self.rabbitmq_pub_client is None")

    def __slave_subscribe_msg__(self):
        """
            从节点订阅逻辑
        :return:
        """
        print("__slave_subscribe_msg__ start ...")
        self.mqtt_statistics_dev_server = MQTTStatisticsDevServer(key="slave_node_statistics",
                                                                  redis_client=self.redis_client,
                                                                  socket_server=self.socket_client,
                                                                  rabbitmq_pub_client=None,
                                                                  node_is_master=self.is_master_node,
                                                                  has_permission=True,
                                                                  )
        self.subscribe_topic_list = []
        for topic in self.subscribe_topic_list:
            self.mqtt_statistics_dev_server.subscribe(topic=topic)
        self.mqtt_statistics_dev_server.start()
        pass

    def create_redis_client(self):
        pool = redis.ConnectionPool(host="127.0.0.1",
                                    db=22, port=6379, password="123456")
        self.redis_client = redis.StrictRedis(connection_pool=pool)
        print("redis client connection is ok")

    def create_rabbitmq_pub_client(self):
        self.rabbitmq_pub_client = RabbitMQPubServer()

    def create_rabbitmq_sub_client(self):
        self.rabbitmq_sub_client = RabbitMQSubServer("127.0.0.1",
                                                     5672,
                                                     "admin",
                                                     "admin123",
                                                     "cluster_virtual_host",
                                                     "cluster_exchange",
                                                     "cluster_queue",
                                                     self.redis_client,
                                                     self.socket_client,
                                                     self.is_master_node,
                                                     self.node_id,
                                                     self.now_node_key,
                                                     )
        threading.Thread(target=self.rabbitmq_sub_client.subscribe_msg,
                         args=()
                         ).start()
        # Process(target=self.rabbitmq_sub_client.subscribe_msg).start()

    def heartbeat_loop_forever(self):
        """
            心跳
        :return:
        """
        pass

    def stop_service(self):
        """
            暂停服务, 不工作状态
        :return:
        """
        pass

    def shutdown_service(self):
        """
            结束服务，并从集群移出当前节点相关信息

        :return:
        """
        pass

    def search_master_node(self):
        """
            查询master节点node_id
        :return: master_node_id:port
        """
        master_node_address = self.redis_client.hget(name="cluster:master_node", key="master_node_id")
        if master_node_address:
            ip, port = (master_node_address.decode()).split(":")
            return ip, int(port)

        return None, None

    def set_master_node(self):
        """
            添加主节点node_id
        :return:
        """
        try:
            self.redis_client.hset(name="cluster:master_node", key="master_node_id", value=self.now_node_key)
            return True
        except Exception as err:
            print("error: {}".format(err))
            return False

    def search_all_node_list(self)->list:
        """
            搜索在线的所有节点服务器
        :return:
        """
        self.redis_client.hgetall(name="cluster:all_node")
        return list()

    def add_node_in_clusters(self):
        """
            将当前节点服务添加到集群中：
            （1）判断当前集群是否存在master节点
                a. 如果不存在，则设立当前节点为master节点
                b. 如果存在，则添加到slave_list中
        :return:
        """
        master_node_id, master_node_port = self.search_master_node()
        if master_node_id:
            """
                存在主节点，当前节点按照从节点操作
            """
            self.is_master_node = False
            print("存在主节点，当前节点按照从节点操作")
            pass

        else:
            # 不存在主节点，当前节点按照主节点操作
            set_master_node_result = self.set_master_node()
            if not set_master_node_result:
                print("set master node result failed! Please restart service")
                return False
            else:
                """
                    设置成功
                """
                pass
        # 添加到all_node节点列表中
        # 查询redis中确认当前节点信息是否已存在于集群中
        bytes_result = self.redis_client.hget(name="cluster:all_node",
                                              key=self.now_node_key)
        if not bytes_result:
            other_node = {}
        else:
            other_node = json.loads(bytes_result.decode())
            print("other_node: {}".format(other_node))
        other_node_key = "{}:{}".format(other_node.get('node_ip'), other_node.get('node_port'))
        if other_node_key == self.now_node_key:
            print(f"now_node_key {self.now_node_key} existed node data, delete other node info!")
            self.redis_client.hdel("cluster:all_node",
                                   self.now_node_key)

        print("self.now_node type : {} , value: {}".format(type(self.now_node), self.now_node))
        self.redis_client.hset(name="cluster:all_node",
                               key="{}:{}".format(self.now_node.get('node_ip'),
                                                  self.now_node.get('node_port')),
                               value=json.dumps(self.now_node)
                               )

        return True

    def delete_node_in_clusters(self):
        """
            从集群中剔除当前节点
        :return:
        """
        self.redis_client.hdel("cluster:all_node", self.now_node_key)
        pass

    # def update_node_info(self):
    #     """
    #         更新当前节点信息，并同步到集群中
    #     :return:
    #     """
    #
    #     pass

    def save_node_socket_info(self):
        """
            存节点信息
        :return:
        """

    pass


class ClusterDataCommand:
    """
        集群服务中数据相关操作集合
    """
    def __init__(self):
        pass

    def add_node(self):
        """
            数据操作：新增节点
        :return:
        """
        pass

    def delete_node(self):
        """
            数据操作：删除节点
        :return:
        """

    def update_node(self):
        """
            数据操作：更新节点
        :return:
        """
        pass

    def search_node(self):
        """
            数据操作：查询节点信息
        :return:
        """
        pass

    def search_node_list(self):
        """
            数据操作：查询所有节点列表信息（包含master，slave节点）
        :return:
        """
        pass

    def search_master_nodes(self):
        """
            数据操作：查询master节点列表信息
        :return:
        """
        pass

    def search_slave_nodes(self):
        """
            数据操作：查询分节点列表
        :return:
        """
        pass

    def pub_processing_device_permissions(self):
        """
            发布处理设备权限
        :return:
        """
        pass

    def sub_processing_device_permissions(self):
        """
            订阅处理设备权限
        :return:
        """
        pass


class RedisCommand:
    def __init__(self, host, port, db, password):
        self.pool = redis.ConnectionPool(host=host, port=port, db=db, password=password)
        self.redis_client = redis.StrictRedis(connection_pool=self.pool)
        print("redis client connection is ok")
        pass

    def check_connection(self):
        pass

    def publish_msg(self, channel, msg):
        """
            发布消息
        :param channel:
        :param msg:
        :return:
        """
        self.redis_client.publish(channel=channel, message=msg)

    def subscribe_msg(self, channel):
        ps = self.redis_client.pubsub()
        ps.subscribe(channel)

    def psubscribe_msg(self, psub_handle_dict: dict, sleep_time: int =1):
        """

        :param psub_handle_dict: { 'channel_key': handle_func}
        :param sleep_time int 1
        :return:
        """
        ps = self.redis_client.pubsub()
        ps.psubscribe(**psub_handle_dict)
        ps.run_in_thread(sleep_time=sleep_time)
        pass


class CASLuaRedis:
    def __init__(self, redis_client):
        self.redis_client = redis_client

    def inc(self, key):
        if not self.redis_client.exists(key):
            return False
        self._lua = self.redis_client.register_script("""
            local next_count = redis.call('get', KEYS[1]) + ARGV[1]
            ARGV[2] = tonumber(ARGV[2])
            if next_count < ARGV[2] then
                next_count = ARGV[2]
            end
            redis.call('set',KEYS[1],next_count)
            return tostring(next_count)
        """)
        return int(self._lua([key], [30, int(time.time())]))


if __name__ == '__main__':
    print(uuid.uuid4())
