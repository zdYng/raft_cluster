"""
连接rabbitmq
"""
import pika


class RabbitMQServer:
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
        # self.channel.queue_declare(queue=self.queue)
        pass

    def publish_msg(self, msg, exchange="mqtt_exchange", ):
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=self.queue,
                                   body=msg)
        pass

    def subscribe_msg(self):
        self.channel.basic_consume(on_message_callback=self.on_msg_callback, queue=self.queue, auto_ack=True)
        self.channel.start_consuming()

    def on_msg_callback(self, ch, method, properties, body):
        print(f"received {body.decode()}")


