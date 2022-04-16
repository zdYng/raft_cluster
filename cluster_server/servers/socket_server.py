"""

"""
import json
import selectors
import socket
import time

import redis

from cluster_server.handlers.server_handlers import ServerHandler


class SocketServer:
    """
        TCP Server
    """
    def __init__(self, server_address: str, server_port: int, redis_client: object):
        self.server_address = server_address
        self.server_port = server_port
        self.server_address_port = f"{self.server_address}:{self.server_port}"

        self.client_address = None
        # 创建socket对象
        self.socket_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # # 设置IP地址复用
        self.socket_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        self.socket_server.bind((self.server_address, self.server_port))
        # 开始监听 最大监听个数 ，最多20个client服务连接
        self.socket_server.listen(20)
        # 设置非阻塞
        self.socket_server.setblocking(False)
        # 自动选择系统可用的I/O复用性能最高的方式
        self.selectors_io = selectors.DefaultSelector()
        self.selectors_io.register(self.socket_server, selectors.EVENT_READ, self.accept_func)
        # 管理客户端个数
        self.all_clients_nums = 0
        self.all_clients_info = set()  # {"node-id-1" , "node-id-2"}

        # redis—client
        if redis_client is not None:
            self.redis_client = redis_client
        else:
            pool = redis.ConnectionPool(host="127.0.0.1",
                                        db=22, port=6379, password="123456")
            self.redis_client = redis.StrictRedis(connection_pool=pool)

        # 处理类
        self.server_handler = ServerHandler(client_socket=self.socket_server,
                                            redis_client=self.redis_client,
                                            address_port=self.server_address_port)
        pass

    def start_server_forever(self):

        print("start server forever")
        while True:
            try:
                events_list = self.selectors_io.select()
                for key, mask in events_list:
                    key.data(key.fileobj, mask)
            except KeyboardInterrupt:
                print("close server ")
                exit()

        self.selectors_io.close()
        pass

    def accept_func(self, sock, mask):
        # 创建客户端连接对象
        client_object, server_address = sock.accept()
        self.client_address = server_address
        print(f"client {server_address} connection is ok")
        self.all_clients_nums += 1
        print("all_clients_nums: {}".format(self.all_clients_nums))
        # 设置连接为非阻塞
        client_object.setblocking(False)
        # 注册
        self.selectors_io.register(client_object, selectors.EVENT_READ, self.read_data)

        # self.send_client_added_device(client_obj=client_object)
        # 维护集群节点信息 - 新增节点

    def read_data(self, client, mask):
        try:
            data = client.recv(65535)
            if data:
                msg = data.decode()
                print("client : type :{} - {}".format(type(msg), msg))
                self.server_handler.receive_msg_handler(msg)

                # 校验几种topic处理对应的业务

                client.sendall(b"call back msg")
                # print("client msg: {}".format(client.raddr))
            else:
                # 删除 client端相关redis数据
                address_port = "{}:{}".format(self.client_address[0], self.client_address[1])
                self.server_handler.redis_del_cluster_node(address_port=address_port)

                print("client {} closed".format(self.client_address))
                self.selectors_io.unregister(client)
                client.close()
                self.all_clients_nums -= 1
                print("all_clients_nums: {}".format(self.all_clients_nums))
                # self.all_clients_nums.remove("")

                # 维护集群节点信息数据 - 减少节点
        except Exception as err:
            print("read_data receive error: {}".format(err))

    def clear_redis_master_info(self, redis_client):
        """
        清理cluster:master_node
        清理cluster:all_node：
            （1）离线时
        :param redis_client:
        :return:
        """
        self.redis_client = redis_client
        # master_node
        master_node_data = self.redis_client.hget(name="cluster:master_node", key="master_node_id")
        if master_node_data:
            self.redis_client.hdel("cluster:master_node", "master_node_id")

        # cluster:all_node
        key = "{}:{}".format(self.server_address,self.server_port)
        master_node_data = self.redis_client.hget(name="cluster:all_node", key=key)
        if master_node_data:
            self.redis_client.hdel("cluster:all_node", key)

    def send_client_added_device(self, client_obj):
        while True:
            self.redis_client.hget("cluster:max_score_slave_node")
            client_obj.send("hello")
            time.sleep(1)

    def recv_other_client(self):
        """
            接收其他客户端的消息
        :return:
        """
        msg, addr = self.socket_server.recvfrom(65535)
        return json.loads(msg), addr


if __name__ == '__main__':
    # SocketServer(server_address="127.0.0.1", server_port=8080).start_server_forever()
    a = (1, 2)
    print(a, type(a))
