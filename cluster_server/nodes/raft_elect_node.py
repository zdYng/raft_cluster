"""
    参考资料：https://raft.github.io/raft.pdf
    选举核心代码
"""
import datetime
import json
import logging
import os
import random
import time

import redis

from cluster_server.servers.socket_node import SocketNode

logging.basicConfig(level=logging.INFO)


class RaftElectNode(object):
    def __init__(self,
                 redis_client,
                 node_address,
                 node_port,
                 now_node_id,
                 other_node_info={}
                 ):
        """
            elect rule:
            (1) leader周期发送心跳，阻止follower发起新的选举
            (2) 在指定时间内follower没收到来自leader的消息，推荐自己为候选人并发起选举
            (3) 在一次选举中，满足>=（N+1）/2 票数，选举为leader
            (4) 在一次选举中，每个节点发出对一个任期内的投票，先到先得原则选择发起投票
        """

        self.role = 'follower'

        self.redis_client = redis_client
        # self.socket_server = now_node_server
        # self.socket_client = now_node_client
        self.socket_node = SocketNode(server_address=node_address,
                                      server_port=node_port)
        self.node_id = now_node_id
        self.node_address = node_address
        self.node_port = node_port
        self.now_node_key = "{}:{}".format(self.node_address, self.node_port)
        self.socket_client_addr = (node_address, node_port)

        self.other_nodes_info = other_node_info  # {node_id: (ip, port) , node_id_2:(ip,port) }
        self.leader_node_id = None
        self.persistent_state_node_id = 0

        # elect terms
        self.current_term = 0
        self.voted_for = None
        self.voted_node_ids: dict = {__id: 0 for __id in self.other_nodes_info}

        self.__load_redis_key()
        # self.__init__redis_key()

        self.log = RaftLog(self.node_id)

        self.now_commit_index = 0  # init 0 , increases
        self.last_applied = 0  # init 0 , increases

        self.next_index = {__id: self.log.last_log_index + 1 for __id in self.other_nodes_info}
        self.match_index = {__id: -1 for __id in self.other_nodes_info}

        self.wait_ms = (1000, 2000)

        self.next_leader_election_time = 0
        self.new_next_leader_election_time()

        self.next_heartbeat_time = 0

        pass

    def new_next_leader_election_time(self):
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)*0.01

    def new_next_heartbeat_time(self):
        self.next_heartbeat_time = time.time() + random.randint(0, 50)*0.1

    def __init__redis_key(self):
        self.redis_client.delete("cluster:raft_persistent_log_{}".format(self.node_id))
        self.redis_client.delete("cluster:raft_elect_node_key", self.node_id)
        value_data = self.redis_client.hget(name="cluster:raft_elect_node_key", key=self.node_id)
        if not value_data:
            self.redis_client.hset(name="cluster:raft_elect_node_key",
                                   key=self.node_id,
                                   value=json.dumps({"current_term": self.current_term, "voted_for": self.voted_for})
                                   )

            pass

    def __load_redis_key(self):
        value_data = self.redis_client.hget(name="cluster:raft_elect_node_key", key=self.node_id)
        if value_data:
            dict_data = json.loads(value_data)
            print("raft_elect_node_key: {}".format(dict_data))
            self.current_term = dict_data.get('current_term')
            self.voted_for = dict_data.get('voted_for')
        else:
            self.__init__redis_key()

    def __add_persistent_log(self):
        new_data = {
                        "current_term": self.current_term,
                        "voted_for": self.voted_for
                    }

        self.redis_client.hset(name="cluster:raft_persistent_log_{}".format(self.node_id),
                               key="{}".format(int(time.time())),
                               value=json.dumps(new_data)
                               )

    def send(self):
        """
            client send
        :return:
        """
        pass

    def recv(self):
        """
            server recv
        :return:
        """
        pass

    def redirect(self, data: dict, addr: tuple):
        """

        :param data:
        :param addr:
        :return:
        """
        if not data:
            return {}
        if data.get('type') == 'client_append_entries':
            print("redirect client_append_entries")
            if self.role != 'leader':
                if self.leader_node_id:
                    print(f"redirect client_append_entries to leader: {self.leader_node_id}")
                    self.socket_node.send(msg=data,
                                          address_port=self.other_nodes_info.get(self.leader_node_id))
                return {}
            else:
                self.socket_client_addr = addr
                return data
        if data.get('dst_node_id') != self.node_id:
            print(f"redirect to: {data.get('dst_node_id')}")
            self.socket_node.send(msg=data, address_port=self.other_nodes_info.get('dst_node_id'))
            return {}
        print(f"redirect ok: {data}")
        return data

    def append_entries(self, data) -> bool:
        """
            复制日志
        :param : {
                    "src_node_id": uuid,
                    "term": 1,
                    "entries": ,
                    "prev_log_index": -1,

                 }
        :return:
        """
        response = {
            "type": "append_entries_response",
            "src_node_id": self.node_id,
            "dst_node_id": data.get('src_node_id'),
            "term": self.current_term,
            "success": False,
        }
        if data.get('term') < self.current_term:
            print("append_entries rule 1: success-> False: smaller term")
            print(f" send append_entries_response to leader: {data.get('src_node_id')}")
            # response['success'] = False
            self.socket_node.send(msg=response,
                                  address_port=self.other_nodes_info.get(data.get('src_node_id')))
            return False

        if data.get('entries'):
            pre_log_index = data.get('pre_log_index', -1)
            pre_log_term = data.get('pre_log_term')
            if pre_log_term != self.log.get_log_term(pre_log_index):
                print("append_entries rule 2. response success->False: index not match or term not match")
                print("append_entries rule 3. log delete entries")
                self.log.delete_entries(prev_log_index=pre_log_index)
            else:
                print("append entries rule 4. success->True: log append entries")
                response['success'] = True
                self.log.append_entries(pre_log_index=pre_log_index, entries=data.get('entries', []))
                self.socket_node.send(msg=response,
                                      address_port=self.other_nodes_info.get(data.get('src_node_id')))
        else:
            # 心跳不需要回复
            print(" heartbeat")

        leader_commit = data.get('leader_commit')
        if leader_commit > self.now_commit_index:
            self.now_commit_index = min(leader_commit, self.log.last_log_index)
            print(f"append entries rule 5. commit_index {self.now_commit_index}")

        self.leader_node_id = data.get('leader_node_id')
        return True

    def request_vote(self, data):
        """
            选举期间候选人发起
        :param data: {
                        "term": 1,
                        ""
                      }
        :return:
        """
        response = {
            "type": "request_vote_response",
            "src_node_id": self.node_id,  # 源
            "dst_node_id": data.get('src_node_id'),  # 目标
            "term": self.current_term,
            "vote_granted": False,

        }
        if data.get('term') < self.current_term:
            """
                接收任期低于当前任期，过期作废
            """
            print("request vote rule 1. response:success->False: smaller term")
            print(f" send request_vote_response to candidate: {data.get('src_node_id')} , "
                  f"result: {response.get('vote_granted')}")
            self.socket_node.send(msg=response,
                                  address_port=self.other_nodes_info.get(data.get('src_node_id')))
            return False

        last_log_index = data.get('last_log_index')
        last_log_term = data.get('last_log_term')

        if self.voted_for is None or self.voted_for == data.get('candidate_node_id'):
            """
                没投过
            """
            print(f"last_log_index:{last_log_index} , self.log.last_log_index: {self.log.last_log_index} ")
            print(f"last_log_term: {last_log_term} , self.log.last_log_term: {self.log.last_log_term}")
            if (last_log_index >= self.log.last_log_index) and (last_log_term >= self.log.last_log_term):
                """
                    接收到的消息中包含的信息均大于等于本地日志信息
                """
                self.voted_for = data.get('src_node_id')
                self.__add_persistent_log()
                response['vote_granted'] = True
                print("request vote rule 2. response:success->True: candidate log is newer")
                print(f"send request_vote_response to candidate: {data.get('src_node_id')} ,"
                      f" addr: {self.other_nodes_info.get(data.get('src_node_id'))}")
                print(f"data: {data}")
                self.socket_node.send(msg=response,
                                      address_port=self.other_nodes_info.get(data.get('src_node_id')))
                return True
            else:
                """
                    否则，失败
                """
                self.voted_for = None
                self.__add_persistent_log()
                print("request vote rule 2. response:success->False: candidate log is older")
                print(f"send request_vote_response to candidate: {data.get('src_node_id')}")
                self.socket_node.send(msg=response,
                                      address_port=self.other_nodes_info.get(data.get('src_node_id')))
                return False
        else:
            """
                投过了
            """
            response['vote_granted'] = False
            print(f"request vote rule 2. success = False: has voted for: {self.voted_for}")
            return True

    def all_node_rule_do(self, data: dict):
        """

        :param data: {"term": 0 , }
        :return:
        """
        print(f"all {self.node_id}".center(100, "-"))
        print("now_commit_index: {} , last_applied: {}".format(self.now_commit_index, self.last_applied))
        if self.now_commit_index > self.last_applied:
            self.last_applied = self.now_commit_index
            print(f"all rule-1: last_applied = {self.last_applied} , need to apply to state machine")

        print("current_term: {} , data['term']: {}".format(self.current_term, data.get('term', -1)))
        if data.get('term', -1) > self.current_term:
            print(f"2. become cluster:slave:follower , receive larger term: {data.get('term')} > {self.current_term}")
            self.new_next_leader_election_time()
            self.role = 'follower'
            self.current_term = data.get('term')
            self.voted_for = None
            self.__add_persistent_log()
            self.leader_node_id = None
            pass
        pass

    def follower_rule_do(self, data: dict):
        """
            slave:follower
        :param data: {
                        "type": "append_entries" ,  # leader: append_entries |candidate: request_vote
                        "src_node_id": node_id ,  # leader_node_id |  candidate_node_id
                      }
        :return:
        """
        print(f"slave:follower {self.node_id}".center(100, '*'))
        reset = False

        if data.get('type') == 'append_entries':
            print("follow-rule-1: append_entries")
            print(f" receive from leader: {data.get('src_node_id')}")
            reset = self.append_entries(data)
        elif data.get('type') == 'request_vote':
            print("follow-rule-1: request_vote")
            print(f" receive from candidate: {data.get('src_node_id')}")
            reset = self.request_vote(data)

        if reset:
            print(" reset next_leader_election_time")
            self.new_next_leader_election_time()

        if time.time() > self.next_leader_election_time:
            print("follow-rule-2: become candidate")
            print(" no request from leader or candidate")
            self.new_next_leader_election_time()
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.__add_persistent_log()
            self.leader_node_id = None
            self.voted_node_ids = {__id: False for __id in self.other_nodes_info}
            return True

    def candidate_rule_do(self, data: dict):
        """
            当前节点为候选节点
            相关规则
        :return:
        """
        now = str(datetime.datetime.now())[:19]
        print(f"{now}--- candidate: {self.node_id}".center(100, "="))
        for dst_node_id in self.other_nodes_info:
            request = {
                "type": "request_vote",
                "src_node_id": self.node_id,
                "dst_node_id": dst_node_id,
                "term": self.current_term,
                "candidate_node_id": self.node_id,
                "last_log_index": self.log.last_log_index,
                "last_log_term": self.log.last_log_term,

            }

            print(f"{now}--- candidate rule 1. send request_vote request to other node_id: {dst_node_id} ,"
                  f" node_addr:{self.other_nodes_info.get(dst_node_id)}")
            self.socket_node.send(msg=request, address_port=self.other_nodes_info.get(dst_node_id))
        if data.get('type') == "request_vote_response":
            """
                follower response 回包
            """
            print(f"data: {data}")
            print(f"{now}--- candidate rule 2. receive request_vote_response from follower: {data.get('src_node_id')}")
            self.voted_node_ids[data.get('src_node_id')] = data.get('vote_granted')
            print(f"voted_node_ids: {self.voted_node_ids.values()}")
            vote_count = sum(list(self.voted_node_ids.values()))
            if vote_count >= ((len(self.other_nodes_info) >> 1) + 1):
                """
                    得票数>= (总票数>>1) +1
                """
                print(f"{now}--- candidate rule 2. become leader: get enough vote")
                self.role = "leader"
                self.voted_for = None
                self.__add_persistent_log()
                self.next_heartbeat_time = 0
                self.next_index = {__id: self.log.last_log_index + 1 for __id in self.other_nodes_info}
                self.match_index = {__id: 0 for __id in self.other_nodes_info}
                print(f"self.next_index: {self.next_index} , self.match_index : {self.match_index}")
                # 将新的主节点信息同步到redis中
                self.redis_client.hset(name="cluster:master_node", key="master_node_id", value=self.now_node_key)
                return True

        elif data.get('type') == "append_entries":
            print(f"{now}--- candidate rule 3.1 receive append_entries request from leader: {data.get('src_node_id')}")
            print(f"{now}--- candidate rule 3.2 become follower")
            self.new_next_leader_election_time()
            self.role = 'follower'
            self.voted_for = None
            self.__add_persistent_log()
            return True

        if time.time() > self.next_leader_election_time:
            print(f"{now}--- candidate: 4. leader_election timeout, and become candidate")
            self.new_next_leader_election_time()
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.node_id
            self.__add_persistent_log()
            self.voted_node_ids = {__id: False for __id in self.other_nodes_info}
            return True
        else:
            return False

    def leader_rule_do(self, data: dict):
        """
            当前节点为领导节点
            相关规则
        :param data:
        :return:
        """
        print(f"leader node id: {self.node_id}".center(100, ">"))
        now_time = time.time()
        now_datetime = str(datetime.datetime.now())[:19]
        print(f"{now_datetime}--- now_time: {now_time} , self.next_heartbeat_time: {self.next_heartbeat_time}")
        if now_time > self.next_heartbeat_time:
            self.new_next_heartbeat_time()
            for dst_node_id in self.other_nodes_info:
                print("self.next_index: {}".format(self.next_index))
                print("self.next_index[dst_node_id]: {}".format(self.next_index[dst_node_id]))
                request = {'type': 'append_entries',
                           'src_node_id': self.node_id,
                           'dst_node_id': dst_node_id,
                           'term': self.current_term,
                           'leader_node_id': self.node_id,
                           'pre_log_index': self.next_index[dst_node_id] - 1,
                           'pre_log_term': self.log.get_log_term(self.next_index[dst_node_id]-1),
                           'entries': self.log.get_entries(self.next_index[dst_node_id]),
                           'leader_commit': self.now_commit_index}
                print(f"leader rule 1. send append_entries to other node id: {dst_node_id}")
                self.socket_node.send(msg=request, address_port=self.other_nodes_info.get(dst_node_id))
            pass

        if data.get('type') == 'client_append_entries':
            data['term'] = self.current_term
            self.log.append_entries(pre_log_index=self.log.last_log_index, entries=[data])
            print('leader rule 2. receive append_entries from client ,log append_entries, log save')

        if data.get('type') == 'append_entries_response':
            print(f"leader rule 3.1 receive append_entries_response from follower: {data.get('src_node_id')}")
            if data.get('success') is False:
                self.next_index[data.get('src_node_id')] -= 1
                print('leader rule 3.2 success = False, next_index - 1')
            else:
                self.match_index[data.get('src_node_id')] = self.next_index.get(data.get('src_node_id'))
                self.next_index[data.get('src_node_id')] = self.log.last_log_index + 1
                print('leader rule 3.2. success->True')
                print(f"  next_index = {str(self.next_index.get(data.get('src_node_id')))}")
                print(f"  match_index = {str(self.match_index.get(data.get('src_node_id')))}")

        while True:
            n = self.now_commit_index + 1
            count = 0
            for __id in self.match_index:
                if self.match_index[__id] >= n:
                    count += 1
                if count >= ((len(self.other_nodes_info) >> 1) + 1):
                    self.now_commit_index = n
                    print("leader rule 4. commit + 1")
                    if self.socket_client_addr:
                        response = {'index': self.now_commit_index}
                        self.socket_node.send(msg=response, address_port=self.socket_client_addr)
                    break
            print(f"leader rule 4. commit = {str(self.now_commit_index)}")
            break

    def run(self):
        while True:
            print('connect...')
            self.socket_node.socket_node.settimeout(5)
            try:
                try:

                    data, addr = self.socket_node.socket_node.recvfrom(65535)
                    data = json.loads(data)
                    now = str(datetime.datetime.now())[:19]
                    print("{}--- recv data: {} , addr: {}".format(now, data, addr))
                except Exception as e:
                    print("exception error: {}".format(e))
                    data, addr = {}, None

                print("before redirect...")
                data = self.redirect(data, addr)

                print(f"before all_node_rule_do data:{data}")
                self.all_node_rule_do(data)

                if self.role == 'follower':
                    print("follower")
                    if self.follower_rule_do(data):
                        continue

                if self.role == 'candidate':
                    print("candidate")
                    if self.candidate_rule_do(data):
                        continue

                if self.role == 'leader':
                    print("leader start")
                    self.leader_rule_do(data)
                    continue

            except KeyboardInterrupt:
                self.socket_node.close()
                # except Exception as err:
                #     print("exception error: {}".format(err))
                #     import traceback
                #     traceback.print_exc()
                #     print("traceback : {}".format(traceback.format_exc()))


class LamportClock(object):
    """
        维持选举过程中偏序性，
        给节点内部消息以及节点间发送/接收消息添加Lomport时间戳
    """
    def __init__(self):
        self.lamport_timestamp = 0

    def happened_in_node(self):
        self.lamport_timestamp += 1

    def send_msg(self, msg: dict):
        msg['lamport_timestamp'] = self.lamport_timestamp

    def receive_msg(self, msg: dict):
        self.lamport_timestamp = max(self.lamport_timestamp, msg.get('lamport_timestamp')) + 1


class RaftLog(object):
    def __init__(self, node_id):
        self.file_name = "./cluster_server/nodes/test/" +\
                         "log/{}.json".format(node_id)
        if os.path.exists(self.file_name):
            with open(self.file_name, "r") as f:
                self.entries = json.load(fp=f)
        else:
            self.entries = []

    @property
    def last_log_index(self):
        return len(self.entries) - 1

    @property
    def last_log_term(self):
        return self.get_log_term(self.last_log_index)

    def get_log_term(self, log_index: int):
        if log_index < 0 or log_index >= len(self.entries):
            return -1
        else:
            return self.entries[log_index]['term']

    def get_entries(self, next_index: int) -> [{}]:
        return self.entries[max(0, next_index):]

    def delete_entries(self, prev_log_index: int):
        if prev_log_index < 0 or prev_log_index >= len(self.entries):
            print("delete_entries delete failed")
            return False
        self.entries = self.entries[:max(0, prev_log_index)]
        self.save()
        print("delete_entries delete ok , ")
        return True

    def append_entries(self, pre_log_index: int, entries: [{}]):
        print("append_entries:  pre_log_index: {} , entries: {}".format(pre_log_index, entries))
        self.entries = self.entries[:max(0, pre_log_index+1)] + entries
        self.save()
        print("RaftLog append_entries save ok")

    def save(self):
        with open(self.file_name, "w") as f:
            json.dump(self.entries, f, indent=4)


if __name__ == '__main__':
    # test = {"0": ("localhost", 10000), "1": ("localhost", 10001)}
    # index = 0
    # print({__id: index + 1 for __id in test})
    # for __id in test:
    #     print("id: {}".format(__id))
    #
    # now_time = time.time()
    # print("now time: {}".format(now_time))
    # print("ms: {}".format(now_time + random.randint(*(100,200))*0.001))
    # print(f"all {uuid.uuid4()}".center(100, "="))
    # print(f"all {uuid.uuid4()}".center(100, "-"))
    # print(f"all {uuid.uuid4()}".center(100, "*"))
    # print(f"all {uuid.uuid4()}".center(100, ">"))
    #
    # print((7>>1) + 1)
    #
    # def test(num):
    #     if num >>1 > 2:
    #         return
    #     else:
    #         return True
    # print(test(6))
    # print(random.randint(0,50)*0.001)

    def test_run():
        pool = redis.ConnectionPool(host="127.0.0.1",
                                    db=22, port=6379, password="123456")
        redis_client = redis.StrictRedis(connection_pool=pool)
        node = RaftElectNode(redis_client=redis_client,
                             node_address="127.0.0.1",
                             node_port=9999,
                             now_node_id="123456"
                             )
        # node.socket_node.server_connect(("127.0.0.1", 9999))
        node.run()
    test_run()
