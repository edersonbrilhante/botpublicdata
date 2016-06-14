# -*- coding:utf-8 -*-
from __future__ import print_function

import motor


def connect(db_name):
    db = motor.motor_tornado.MotorClient('localhost', 27017)[db_name]
    return db
