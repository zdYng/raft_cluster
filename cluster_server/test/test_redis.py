import json
import time
import uuid

import redis


class CASLuaRedis:
    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.lua_script = self.redis_client.register_script("""
            local next_count = redis.call('get', KEYS[1]) + ARGV[1]
            ARGV[2] = tonumber(ARGV[2])
            if next_count < ARGV[2] then
                next_count = ARGV[2]
            end
            redis.call('set',KEYS[1],next_count)
            return tostring(next_count)
        """)

    def inc(self, key):
        if not self.redis_client.exists(key):
            self.redis_client.set(key, 0)

        return int(self.lua_script([key], [30, int(time.time())]))


def test_cas():
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    cas_redis = CASLuaRedis(redis_client=redis_client).inc(key="test_cas")
    print(cas_redis)


def test_redis():
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    dict_map = {"{}".format(uuid.uuid4()): 0,
                "{}".format(uuid.uuid4()): 1
                }
    redis_client.hset(name="dict_name", key="key_1", value=json.dumps(dict_map))
    redis_client.hset(name='dict_name', key='key_2', value='bbbb')
    d1 = json.loads(redis_client.hget(name="dict_name", key="key_1").decode())
    print(d1, type(d1))
    print(redis_client.hget(name='dict_name', key="key_2"))

    dict_2 = {"key_1": 123, "key_2": "asd", "key_3": 123}
    redis_client.hset(name="cluster:dict_name_2", key="key", value=json.dumps(dict_2))
    print(json.loads(redis_client.hget(name="cluster:dict_name_2", key="key")))


def test_pub_sub():
    """

    :return:
    """
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    pubsub = redis_client.pubsub()
    pubsub.subscribe('test')
    for item in pubsub.listen():
        print(item)


def test_dict_value():
    a = {
        "a": 0,
        "b": 1,
        "c": 0,
    }
    print(a.items())
    result = False
    for i in a.items():
        if i[1] == 0:
            return False
    return True

    print(b[0])


def test_redis_2():
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)

    # set
    a1 = str(uuid.uuid4())
    redis_client.hset(name="cluster:elect_node_score", key=a1, value=0)
    redis_client.hset(name="cluster:elect_node_score", key=str(uuid.uuid4()), value=10)
    redis_client.hset(name="cluster:elect_node_score", key=str(uuid.uuid4()), value=20)
    redis_client.hset(name="cluster:elect_node_score", key=str(uuid.uuid4()), value=30)
    a = redis_client.hgetall(name="cluster:elect_node_score")
    keys = list(a.keys())
    value_list = []
    max_score_node_id = None
    max_score = 0
    for key in keys:
        value_list.append(a.get(key))
        if int(a.get(key).decode())>max_score:
            max_score = int(a.get(key).decode())
            max_score_node_id = key.decode()
    print(max_score)
    print(max_score_node_id)
    print(a, type(a))

    # del


def del_data():
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)

    a = redis_client.hgetall(name="cluster:elect_node_score")
    keys_bytes = list(a.keys())
    keys = [i.decode() for i in keys_bytes]
    print(keys)
    for key in keys:
        redis_client.hdel("cluster:elect_node_score", key)


pool = redis.ConnectionPool(host="127.0.0.1",
                            db=22, port=6379, password="123456")
redis_client = redis.StrictRedis(connection_pool=pool)


def test_hget():
    data = redis_client.hget(name="cluster:master_statistics_uav", key="all_uav_id_list")
    if data:
        json_data = json.loads(data.decode())
        print(type(json_data.get('all_uav_id_list')[0]))
    data2 = redis_client.hget(name="cluster:master_statistics_uav", key="100011")
    if data2:
        print(data2)

def test_hgetall():
    data = redis_client.hgetall(name="cluster:all_node")
    print(data)
    print(type(data))
    print(data.keys())
    master_data = redis_client.hget(name="cluster:master_node", key="master_node_id")
    slave_node_list = []
    if master_data:
        print("master_data: {}".format(master_data.decode()))
        for i in data.keys():
            print(i.decode())
            print(json.loads(data.get(i).decode()))
            value_dict = json.loads(data.get(i).decode())
            slave_node = "{}:{}".format(value_dict.get('node_ip'), value_dict.get('node_port'))
            if slave_node != master_data:
                slave_node_list.append(value_dict)
    print(f"slave_node_list: {slave_node_list}")


def test_redis_hset_expire():
    while True:
        redis_client.hset("cluster:online_node", "127.0.0.1:9090", True)
        redis_client.hset("cluster:online_node", "127.0.0.1:9091", True)
        redis_client.expire("cluster:online_node", 5)
        print("save data: cluster:online_node 127.0.0.1:9090 True")
        time.sleep(4)


def test_redis_set_expire():
    while True:
        redis_client.set("cluster:online_node_127.0.0.1:9090", "127.0.0.1:9090")
        redis_client.expire("cluster:online_node_127.0.0.1:9090", 5)
        redis_client.set("cluster:online_node_127.0.0.1:9091", "127.0.0.1:9091")
        redis_client.expire("cluster:online_node_127.0.0.1:9091", 5)
        print("save data: cluster:online_node 127.0.0.1:9090/9091 ")
        time.sleep(10)


if __name__ == '__main__':
    # test_pub_sub()
    # test_redis()
    # test_hgetall()
    # for i in range(0, 2):
    #     test_cas()
    test_redis_set_expire()