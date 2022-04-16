import json
import time
import uuid

import pika


class Tracer:
    def __init__(self, func):
        self.id = uuid.uuid1()
        self.call_nums = 0
        self.func = func
        self.start_time = time.time()
        pass

    def __call__(self, *args, **kwargs):
        now_time_stamp = time.time()
        if not (now_time_stamp - self.start_time) < 1:
            print("self.id: {} , func_name: {} , call_nums: {}".format(self.id, self.func, self.call_nums))
            self.clean_and_init()
        self.call_nums += 1
        self.func(*args, **kwargs)

        pass

    def clean_and_init(self):
        self.start_time = time.time()
        self.call_nums = 0
        print(f"clean_and_init, start_time: {self.start_time} , call_nums: {self.call_nums}")


class RabbitMQPubServer:
    def __init__(self, username: str, pwd: str, virtual_host: str, host: str, port: int, queue: str):
        self.username = username
        self.pwd = pwd
        self.virtual_host = virtual_host
        self.host = host
        self.port = port
        self.queue = queue
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                      port=self.port,
                                                                      virtual_host=self.virtual_host,
                                                                      credentials=pika.PlainCredentials(
                                                                          username=self.username,
                                                                          password=self.pwd)))
        self.channel = self.conn.channel()
        print(f"self.channel: {self.channel}")
        self.channel.queue_declare(queue=self.queue, durable=True)
        num = 0
        while True:
            time.sleep(1)

            msg = {'timestamp': int(time.time()), 'num': num}
            self.publish_msg(self, msg=json.dumps(msg), exchange="cluster_exchange")
            num += 1

    @Tracer
    def publish_msg(self, msg, exchange="cluster_exchange"):
        self.channel.basic_publish(exchange=exchange,
                                   routing_key="",
                                   body=msg)


host = "127.0.0.1"
port = 5672
username="admin"
pwd="admin123"
virtual_host="cluster_virtual_host"
queue="cluster_queue"

if __name__ == '__main__':
    rabbitmq = RabbitMQPubServer(host="127.0.0.1",
                              port=5672,
                              username="admin",
                              pwd="admin123",
                              virtual_host="cluster_virtual_host",
                              queue="cluster_queue"
                              )
    # conn = pika.BlockingConnection(pika.ConnectionParameters(host=host,
    #                                                          port=port,
    #                                                          virtual_host=virtual_host,
    #                                                          credentials=pika.PlainCredentials(
    #                                                              username=username,
    #                                                              password=pwd)))
    # conn.channel().basic_publish(exchange="mqtt_exchange",
    #                              routing_key="mqtt_queue",
    #                              body="hahahah")

