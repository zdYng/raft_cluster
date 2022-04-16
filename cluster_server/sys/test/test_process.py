#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from multiprocessing import Process


def fun_test(id):
    print(f"id : {id}")


def run_process():
    process = [
        Process(target=fun_test, args=(1,)),
        Process(target=fun_test, args=(2,))
    ]
    [p.start() for p in process]
    [p.join() for p in process]


if __name__ == '__main__':
    run_process()
