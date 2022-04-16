import time
from multiprocessing import Process


def while_loop():
    times = 0
    while True:
        time.sleep(2)
        print("sleep 2 s")
        times += 1
        if times > 10:
            break


if __name__ == '__main__':
    process = Process(target=while_loop, args=())
    process.start()
    print(process.is_alive())
    time.sleep(2)
    print(process.terminate())
    print(process.is_alive())

