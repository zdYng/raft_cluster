"""
    通用消息Topic处理

"""


class CommonMessageHandler:
    def __init__(self):
        # 节点心跳topic
        self.heartbeats_topic = "cluster/heartbeats"
        # 健康状态Topic
        self.health_status_topic = "cluster/health_status"
        # 工作状态Topic
        self.working_status_topic = "cluster/working_status"
        # 负载状态Topic
        self.load_average_status_topic = "cluster/load_average_status"
        # 节点信息通知Topic
        self.node_info_topic = "cluster/node_info"

        pass

    def ack_msg(self):

        pass

