#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import json
import os


class RaftLog(object):
    def __init__(self, node_id):
        self.file_name = "./" + "node-{}".format(node_id) + "/log.json"
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
            return None
        else:
            return self.entries[log_index]['term']

    def get_entries(self, next_index: int) -> [{}]:
        return self.entries[max(0, next_index):]

    def delete_entries(self, prev_log_index: int):
        if prev_log_index < 0 or prev_log_index >= len(self.entries):
            return False
        self.entries = self.entries[:max(0, prev_log_index)]
        self.save()
        return True

    def append_entries(self, pre_log_index: int, entries: [{}]):
        self.entries = self.entries[:max(0, pre_log_index+1)] + entries
        self.save()

    def save(self):
        with open(self.file_name, "w") as f:
            json.dump(self.entries, f, indent=4)
