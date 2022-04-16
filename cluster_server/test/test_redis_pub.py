import datetime
import json
import time

import redis


def test_pub():
    """

    :return:
    """
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    while True:
        now = str(datetime.datetime.now())[:19]
        print("sending {}".format(now))
        redis_client.publish("test", (str(now)))
        time.sleep(1)


def test_get_list():
    """

    :return:
    """
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    a_list = [1,2,3]
    redis_client.hset(name="cluster:slave_statistics_uav", key="127.0.0.1:8080", value=json.dumps(a_list))
    value = redis_client.hget(name="cluster:slave_statistics_uav", key="127.0.0.1:8080")
    print(json.loads(value), type(json.loads(value)))


if __name__ == '__main__':
    test_get_list()

