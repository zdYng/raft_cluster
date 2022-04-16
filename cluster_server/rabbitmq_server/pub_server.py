import json
import time
import uuid

import pika


class RabbitMQPubServer:
    def __init__(self,
                 host: str="127.0.0.1",
                 port: int=5672,
                 username: str="admin",
                 pwd: str="admin123",
                 virtual_host: str="cluster_virtual_host",
                 exchange:str="cluster_exchange",
                 queue: str="cluster_queue"):
        self.username = username
        self.pwd = pwd
        self.virtual_host = virtual_host
        self.host = host
        self.port = port
        self.exchange = exchange
        self.queue = queue
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=self.host,
                                                                      port=self.port,
                                                                      virtual_host=self.virtual_host,
                                                                      credentials=pika.PlainCredentials(
                                                                          username=self.username,
                                                                          password=self.pwd)))
        self.channel = self.conn.channel()

        # self.channel.exchange_declare(exchange=self.exchange, exchange_type="fanout")
        self.channel.queue_declare(queue=self.queue, durable=True)

        # num = 0
        # while True:
        #     time.sleep(1)
        #
        #     msg = {'timestamp': int(time.time()), 'num': num}
        #     self.publish_msg(self, msg=json.dumps(msg), exchange="mqtt_exchange")
        #     num += 1

    def publish_msg(self, msg, exchange="cluster_exchange"):
        print("publish_msg start")

        self.channel.basic_publish(exchange=exchange,
                                   routing_key="",
                                   body=msg)


