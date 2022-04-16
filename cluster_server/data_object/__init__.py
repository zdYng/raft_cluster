#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from dataclasses import asdict, dataclass


@dataclass
class BasicDataObject(object):

    @property
    def __as_dict__(self) -> dict:
        return asdict(self)
