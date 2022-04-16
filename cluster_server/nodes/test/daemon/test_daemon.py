#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import time
import threading


def test_daemon():
    print("start daemon test")
    a = 0
    while a < 6:
        print("a: {}".format(a))
        time.sleep(1)
        a += 1
    print("daemon test end !")

def main():
    print("main thread")
    t1 = threading.Thread(target=test_daemon, args=())
    t1.setDaemon(True)
    t1.start()
    time.sleep(1)
    print("main thread end")

if __name__ == '__main__':
    main()