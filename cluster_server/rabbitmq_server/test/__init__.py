import time
import uuid


class Tracer:
    def __init__(self, func):
        self.id = uuid.uuid1()
        self.call_nums = 0
        self.func = func
        self.start_time = time.time()
        pass

    def __call__(self, *args, **kwargs):
        now_time_stamp = time.time()
        if not (now_time_stamp - self.start_time) < 1:
            print("self.id: {} , func_name: {} , call_nums: {}".format(self.id, self.func, self.call_nums))
            self.clean_and_init()
        self.call_nums += 1
        self.func(*args, **kwargs)

        pass

    def clean_and_init(self):
        self.start_time = time.time()
        self.call_nums = 0
        print(f"clean_and_init, start_time: {self.start_time} , call_nums: {self.call_nums}")
