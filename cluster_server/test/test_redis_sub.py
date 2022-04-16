import redis


def test_sub_func(msg):
    print("测试-订阅消息成功: {}".format(msg.get('data').decode()))


def test_pub_sub():
    """

    :return:
    """
    pool = redis.ConnectionPool(host="127.0.0.1",
                                db=22, port=6379, password="123456")
    redis_client = redis.StrictRedis(connection_pool=pool)
    pubsub = redis_client.pubsub()
    map_ps = {
        'test': test_sub_func,
    }
    # pubsub.subscribe('test')
    pubsub.psubscribe(**map_ps)
    pubsub.run_in_thread(sleep_time=1)
    # for item in pubsub.listen():
    #     print(item)


if __name__ == '__main__':
    test_pub_sub()

