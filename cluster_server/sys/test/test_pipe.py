#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import time
from multiprocessing import Pipe, Process


def reader_pipe(pipe):
    output_p, input_p = pipe
    input_p.close()
    while True:
        try:
            msg = output_p.recv()
            # print("reader msg: {}".format(msg))
        except Exception as err:
            print("reader pipe error: {}".format(err))
            break


def writer_pipe(count, input_p):
    for ii in range(0, count):
        input_p.send(ii)


if __name__ == '__main__':
    for count in [10**5, 10**6, 10**7]:
        output_p, input_p = Pipe()
        reader_p = Process(target=reader_pipe, args=((output_p, input_p),))
        reader_p.start()

        output_p.close()
        # _start = datetime.datetime.now()[:19]
        # print("now datetime: {}".format(_start))
        _start_timestamp = time.time()
        writer_pipe(count, input_p)
        input_p.close()
        reader_p.join()
        print("send {} numbers to Pipe() took {} seconds".format(count, time.time()-_start_timestamp))


