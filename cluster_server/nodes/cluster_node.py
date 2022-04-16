"""
master - slave
master - node
slave  - node
"""
import json
import time

from cluster_server import NodeHealthStatusEnum, NodeWorkingStatusEnum, NodeLoadAverageStatusEnum


class ElectMode:
    """
        选举模式：
        触发机制：
         （1）服务初始化
         （2）服务运行期间无法与master保持连接
    """
    def __init__(self, redis_client):
        # redis客户端 ， 使用RedisCommand.redis_client
        self.redis_client = redis_client

        # 状态枚举
        self.NodeHealthStatusEnum = NodeHealthStatusEnum
        self.NodeWorkingStatusEnum = NodeWorkingStatusEnum
        self.NodeLoadAverageStatusEnum = NodeLoadAverageStatusEnum
        pass

    def elect_master_node(self, now_node_info):
        """
            选举节点:
            当主节点失效时，slave节点集群会触发选举机制，步骤如下：
            （1）各slave节点状态信息打分，并同步到其他节点；
            （2）各节点根据比分投票给分最高的节点；
            （3）投票结束，投票选举出票数最多的节点，并更改节点信息以及启动主节点相关服务。
        :return:
        """
        # 节点状态信息打分存入redis
        now_node_status_score = self.score_node_status(now_node_info)
        if now_node_status_score < 10:
            print("当前节点为非法节点，无法进行投票操作")
            return None
        # 所有节点都打分完成，开始投票
        check_vote_result = self.check_vote_completion(sleep_time=1, check_times_limit=10)
        if check_vote_result:
            """
                如果成功，选出分最高的节点
                {"max_score": max_score, "max_score_node_id": max_score_node_id}
            """
            max_score_node = self.choose_max_score_node()
            now_node_id = now_node_info.get('node_id')
            if now_node_id == max_score_node.get('max_score_node_id'):
                print("最高分为当前节点，返回数据:{}".format(max_score_node))
                return max_score_node
            else:
                # 接收到新master节点重启socket-client通知
                print("不是当前节点，不做任何操作, 返回None")
                return None
        return None

    def elect_manage_dev_slave_node(self, now_node_info):
        """

        :param now_node_info:
        :return:
        """
        now_node_status_score = self.score_node_status(now_node_info)
        if now_node_status_score < 10:
            return None
        # 所有节点都打分完成，开始投票
        check_vote_result = self.check_vote_completion(sleep_time=1, check_times_limit=10)
        if check_vote_result:
            """
                如果成功，选出分最低的节点
                {"min_score": min_score, "min_score_node_id": min_score_node_id}
            """
            lowest_score_node = self.choose_lowest_score_node()
            now_node_id = now_node_info.get('node_id')
            if now_node_id == lowest_score_node.get('min_score_node_id'):
                print("最低分为当前节点，返回数据:{}".format(lowest_score_node))
                return lowest_score_node
            else:
                print("不是当前节点，不做任何操作")
                return None
        return None

    def score_node_status(self, now_node_info):
        """

        :param now_node_info: {
            "node_id": "",  # uuid
            "node_ip": "",
            "node_port": ,
            "health_status":,
            "working_status": ,
            "load_average_status": ,
            "desc": ,
        }
        :return:
        """
        score = 0

        # 健康状态打分

        if now_node_info.get('health_status') in [self.NodeHealthStatusEnum.__ERROR_HEALTH_STATUS__,
                                                  self.NodeHealthStatusEnum.__OFFLINE_HEALTH_STATUS__]:
            return -1
        else:
            score += 10

        # 工作状态打分
        if now_node_info.get('working_status') == self.NodeWorkingStatusEnum.__ERROR_WORKING_STATUS__:
            # 错误状态
            return -1

        elif now_node_info.get('working_status') == self.NodeWorkingStatusEnum.__IDLE_WORKING_STATUS__:
            score += 60
            return score
        elif now_node_info.get('working_status') == self.NodeWorkingStatusEnum.__BUSY_WORKING_STATUS__:
            score += 20
        else:
            # 其他异常工作状态
            return -1

        # 负载状态
        if now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__ERROR_LOAD_AVERAGE_STATUS__:
            # 错误状态
            return 0
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__ZERO_LOAD_AVERAGE_STATUS__:
            score += 30
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__LOW_LOAD_AVERAGE_STATUS__:
            score += 20
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__COMMON_LOAD_AVERAGE_STATUS__:
            score += 10
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__FULL_LOAD_AVERAGE_STATUS__:
            score += 0
        else:
            # 其他错误负载状态
            return -1
        self.redis_client.hset(name="cluster:elect_node_score",
                               key="{}".format(now_node_info.get("node_id")),
                               value=score)
        return score

    def check_vote_completion(self, sleep_time=1, check_times_limit=10) -> bool:
        """
            校验所有投票是否完成
        :return:
        """
        check_times = 0
        while True:
            """
                vote_completion: {node-id: 0 , }
                0 - 未完成
                1 - 已完成
            """
            if check_times > check_times_limit:
                print("超过校验尝试次数")
                return False
            vote_completion = json.loads(self.redis_client.hget(name="cluster:elect_node_vote",
                                                                key="vote_completion").decode())
            result = True
            for item in vote_completion.items:
                if item[1] == 0:
                    result = False
                    check_times += 1
                    break
            if result:
                return True
            time.sleep(sleep_time)
        return True

    def choose_max_score_node(self):
        """
            选出最高分节点
            cluster:elect_node_score key: node-id  0
        :return:
        """
        elect_node_score_dict = self.redis_client.hget(name="cluster:elect_node_score")
        keys = list(elect_node_score_dict.keys())
        value_list = []
        max_score_node_id = None
        max_score = 0
        for key in keys:
            value_list.append(elect_node_score_dict.get(key))
            if int(elect_node_score_dict.get(key).decode()) > max_score:
                max_score = int(elect_node_score_dict.get(key).decode())
                max_score_node_id = key.decode()
        print(max_score)
        print(max_score_node_id)

        return {"max_score": max_score, "max_score_node_id": max_score_node_id}

    def choose_lowest_score_node(self):
        """
            选出最低分节点
        :return:
        """
        elect_node_score_dict = self.redis_client.hget(name="cluster:elect_node_score")
        keys = list(elect_node_score_dict.keys())
        value_list = []
        min_score_node_id = None
        min_score = 100
        for key in keys:
            value_list.append(elect_node_score_dict.get(key))
            if int(elect_node_score_dict.get(key).decode()) < min_score:
                min_score = int(elect_node_score_dict.get(key).decode())
                min_score_node_id = key.decode()
        print("min score: {}".format(min_score))
        print("min score node id: {}".format(min_score_node_id))

        return {"min_score": min_score, "min_score_node_id": min_score_node_id}


class AssignNodePermissions:
    """
        分配节点处理权限
    """
    def __init__(self, redis_client):
        self.redis_client = redis_client
        # 状态枚举
        self.NodeHealthStatusEnum = NodeHealthStatusEnum
        self.NodeWorkingStatusEnum = NodeWorkingStatusEnum
        self.NodeLoadAverageStatusEnum = NodeLoadAverageStatusEnum
        pass

    def manage_devices_permission(self, node_is_master=True):
        """
            管理设备权限
        :return:
        """
        if node_is_master:
            """
                当前节点为主节点
            """
            data = self.redis_client.hgetall(name="cluster:all_node")
            if data:
                master_node = self.redis_client.hget(name="cluster:master_node", key="master_node_id")
                if master_node:
                    master_node_addr = master_node.decode()
                    # print("master_node_addr: {}".format(master_node_addr))
                    max_score = 0
                    max_score_node_id = None
                    max_score_address = None
                    max_score_node_info = {}
                    for i in data.keys():
                        value_dict = json.loads(data.get(i).decode())

                        slave_node = "{}:{}".format(value_dict.get('node_ip'), value_dict.get('node_port'))
                        if slave_node != master_node_addr:
                            # 算节点分数
                            score = self.score_node_status(now_node_info=value_dict)
                            slave_node_id = value_dict.get('node_id')
                            if score > 0 and slave_node_id:
                                self.redis_client.hset(name="cluster:slave_node_score",
                                                       key=slave_node_id,
                                                       value=score)
                                if max_score < score:
                                    max_score = score
                                    max_score_node_id = slave_node_id
                                    max_score_address = slave_node
                                    max_score_node_info = value_dict
                    if max_score > 0 and max_score_node_id:
                        print("max_score: {} , max_score_node_id: {}, max_score_address: {}".format(max_score,
                                                                                                    max_score_node_id,
                                                                                                    max_score_address))

                        return {"max_score": max_score,
                                "max_score_node_id": max_score_node_id,
                                "max_score_node_address": max_score_address,
                                "max_score_node_info": max_score_node_info,
                                }
            return None

        else:
            """
                当前节点为从节点
            """
            return None
        pass

    def score_node_status(self, now_node_info):
        """

        :param now_node_info: {
                                "node_id": "f5940b56-4b69-40da-afab-8923d1f93f3e",
                                "node_ip": "127.0.0.1",
                                "node_port": 9191,
                                "health_status": 1,
                                "working_status": 0,
                                "load_average_status": 0,
                                "desc": "test one",
                                "subscribe_devices":
                                 {
                                    "subscribe_uav_id_list": [],
                                    "subscribe_uap_id_list": []
                                 }
                            }
        :return:
        """
        score = 0

        # 健康状态打分

        if now_node_info.get('health_status') in [self.NodeHealthStatusEnum.__ERROR_HEALTH_STATUS__,
                                                  self.NodeHealthStatusEnum.__OFFLINE_HEALTH_STATUS__]:
            return -1
        else:
            score += 10

        # 工作状态打分
        if now_node_info.get('working_status') == self.NodeWorkingStatusEnum.__ERROR_WORKING_STATUS__:
            # 错误状态
            return -1

        elif now_node_info.get('working_status') == self.NodeWorkingStatusEnum.__IDLE_WORKING_STATUS__:
            score += 60
            return score
        elif now_node_info.get('working_status') == self.NodeWorkingStatusEnum.__BUSY_WORKING_STATUS__:
            score += 20
        else:
            # 其他异常工作状态
            return -1

        # 负载状态
        if now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__ERROR_LOAD_AVERAGE_STATUS__:
            # 错误状态
            return 0
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__ZERO_LOAD_AVERAGE_STATUS__:
            score += 30
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__LOW_LOAD_AVERAGE_STATUS__:
            score += 20
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__COMMON_LOAD_AVERAGE_STATUS__:
            score += 10
        elif now_node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__FULL_LOAD_AVERAGE_STATUS__:
            score += 0
        else:
            # 其他错误负载状态
            return -1
        return score

    def update_node_info(self, now_node_addr: str, now_node_info):

        # 更新cluster:all_node
        self.redis_client.hset(name="cluster:all_node", key=now_node_addr, value=json.dumps(now_node_info))
        self.update_node_status(node_id=now_node_info.get('node_id'),
                                status_type="health_status",
                                status_value=now_node_info.get("health_status")
                                )
        self.update_node_status(node_id=now_node_info.get('node_id'),
                                status_type="working_status",
                                status_value=now_node_info.get("health_status")
                                )
        self.update_node_status(node_id=now_node_info.get('node_id'),
                                status_type="health_status",
                                status_value=now_node_info.get("load_average_status")
                                )

    def update_node_status(self, node_id, status_type, status_value):
        if status_type == "health_status":
            old_status_value_bytes = self.redis_client.hget(name="cluster:slave_node:health_status", key=node_id)
            if old_status_value_bytes:
                old_status_value = old_status_value_bytes.decode()
                if status_value not in [self.NodeHealthStatusEnum.__ERROR_HEALTH_STATUS__,
                                        self.NodeHealthStatusEnum.__OFFLINE_HEALTH_STATUS__,
                                        self.NodeHealthStatusEnum.__ONLINE_HEALTH_STATUS__,
                                        ]:
                    print("update_node_status false, status_type : {} ,status_value {} not in NodeHealthStatusEnum".format(status_type,
                                                                                               status_value))
                    return False
                if status_value != old_status_value:
                    self.redis_client.hset(name="cluster:slave_node:health_status", key=node_id, value=status_type)
                    print("update_node_status ok , now status_type: {} status_value: {}".format(status_type, status_value))
                    return True
                else:
                    print("update_node_status failed, status_type: {} ,status_value: {} is equal".format(status_type, status_value))
                    return True
        elif status_type == "working_status":
            old_status_value_bytes = self.redis_client.hget(name="cluster:slave_node:working_status", key=node_id)
            if old_status_value_bytes:
                old_status_value = old_status_value_bytes.decode()
                if status_value not in [self.NodeWorkingStatusEnum.__ERROR_WORKING_STATUS__,
                                        self.NodeWorkingStatusEnum.__IDLE_WORKING_STATUS__,
                                        self.NodeWorkingStatusEnum.__BUSY_WORKING_STATUS__,
                                        ]:
                    print("update_node_status false, status_type : {} ,status_value {} not in NodeWorkingStatusEnum".format(status_type,
                                                                                                                            status_value))
                    return False
                if status_value != old_status_value:
                    self.redis_client.hset(name="cluster:slave_node:working_status", key=node_id, value=status_type)
                    print("update_node_status ok , now status_type: {} status_value: {}".format(status_type, status_value))
                    return True
                else:
                    print("update_node_status failed, status_type: {} ,status_value: {} is equal".format(status_type, status_value))
                    return True

            pass
        elif status_type == "load_average_status":
            old_status_value_bytes = self.redis_client.hget(name="cluster:slave_node:load_average_status", key=node_id)
            if old_status_value_bytes:
                old_status_value = old_status_value_bytes.decode()
                if status_value not in [self.NodeLoadAverageStatusEnum.__ERROR_LOAD_AVERAGE_STATUS__,
                                        self.NodeLoadAverageStatusEnum.__ZERO_LOAD_AVERAGE_STATUS__,
                                        self.NodeLoadAverageStatusEnum.__LOW_LOAD_AVERAGE_STATUS__,
                                        self.NodeLoadAverageStatusEnum.__COMMON_LOAD_AVERAGE_STATUS__,
                                        self.NodeLoadAverageStatusEnum.__FULL_LOAD_AVERAGE_STATUS__,
                                        ]:
                    print("update_node_status false, status_type : {} ,status_value {} not in NodeLoadAverageStatusEnum".format(status_type,
                                                                                                                                status_value))
                    return False
                if status_value != old_status_value:
                    self.redis_client.hset(name="cluster:slave_node:load_average_status", key=node_id, value=status_type)
                    print("update_node_status ok , now status_type: {} status_value: {}".format(status_type, status_value))
                    return True
                else:
                    print("update_node_status failed, status_type: {} ,status_value: {} is equal".format(status_type, status_value))
                    return True
            self.redis_client.hset(name="cluster:slave_node:load_average_status", key=node_id, value=status_type)
            pass
        else:
            pass

    def fix_node_info(self, node_info:dict, added_device_type:str, added_device_id:int):
        """
            根据相关信息，修正原有节点信息
        :param node_info: {"node_id": "f5940b56-4b69-40da-afab-8923d1f93f3e",
                           "node_ip": "127.0.0.1",
                           "node_port": 9191,
                           "health_status": 1,
                           "working_status": 0,
                           "load_average_status": 0,
                           "desc": "test one",
                           "subscribe_devices":
                          {"subscribe_uav_id_list": [], "subscribe_uap_id_list": []}}

        :param added_device_type: ['uav', 'uap']
        :param added_device_id: int
        :return:
        """
        if added_device_type not in ['uav', 'uap']:
            return False
        if added_device_id is None:
            return False
        # working_status
        if node_info.get('working_status') == self.NodeWorkingStatusEnum.__IDLE_WORKING_STATUS__:
            node_info['working_status'] = self.NodeWorkingStatusEnum.__BUSY_WORKING_STATUS__

        # load_average_status
        if node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__ZERO_LOAD_AVERAGE_STATUS__:
            node_info['load_average_status'] = self.NodeLoadAverageStatusEnum.__LOW_LOAD_AVERAGE_STATUS__

        if node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__LOW_LOAD_AVERAGE_STATUS__:
            if added_device_type == 'uav':
                sub_uav_id_list = node_info.get('subscribe_devices',{}).get('subscribe_uav_id_list', [])
                sub_uav_id_list.append(added_device_id)
                node_info['subscribe_devices']['subscribe_uav_id_list'] = sub_uav_id_list
                if len(sub_uav_id_list) > 2:
                    node_info['load_average_status'] = self.NodeLoadAverageStatusEnum.__COMMON_LOAD_AVERAGE_STATUS__
            else:
                sub_uap_id_list = node_info.get('subscribe_devices', {}).get('subscribe_uap_id_list', [])
                sub_uap_id_list.append(added_device_id)
                node_info['subscribe_devices']['subscribe_uap_id_list'] = sub_uap_id_list
                if len(sub_uap_id_list) > 2:
                    node_info['load_average_status'] = self.NodeLoadAverageStatusEnum.__COMMON_LOAD_AVERAGE_STATUS__
        elif node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__COMMON_LOAD_AVERAGE_STATUS__:
            if added_device_type == 'uav':
                sub_uav_id_list = node_info.get('subscribe_devices',{}).get('subscribe_uav_id_list', [])
                sub_uav_id_list.append(added_device_id)
                node_info['subscribe_devices']['subscribe_uav_id_list'] = sub_uav_id_list
                if len(sub_uav_id_list) > 5:
                    node_info['load_average_status'] = self.NodeLoadAverageStatusEnum.__FULL_LOAD_AVERAGE_STATUS__
            else:
                sub_uap_id_list = node_info.get('subscribe_devices', {}).get('subscribe_uap_id_list', [])
                sub_uap_id_list.append(added_device_id)
                node_info['subscribe_devices']['subscribe_uap_id_list'] = sub_uap_id_list
                if len(sub_uap_id_list) > 5:
                    node_info['load_average_status'] = self.NodeLoadAverageStatusEnum.__FULL_LOAD_AVERAGE_STATUS__
        elif node_info.get('load_average_status') == self.NodeLoadAverageStatusEnum.__FULL_LOAD_AVERAGE_STATUS__:
            """
                do nothing
            """
            pass

        return node_info


