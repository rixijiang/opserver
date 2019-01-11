# -*- coding:utf-8 -*-
import os

CON_RECV = {'BUFFSIZE': 1024, 'ADDR': ('', 65001)}
CON_SEND = {'BUFFSIZE': 1024, 'ADDR': ('', 65002)}

MCH_REC_PORT = 65002
# MYSQL_DATABASE = {'host': '192.168.56.101', 'port': 3306, 'user': 'root', 'password': '123456', 'db': 'op'}

MYSQL_DATABASE = {'host': '1.1.9.246', 'port': 3306, 'user': 'test_op', 'password': '123456', 'db': 'test_op'}

# 获取初始化创建的日志目录和json文件存放目录
FILE_PATH = os.path.dirname(os.path.dirname(__file__))
LOG_FILE = os.path.join(FILE_PATH, 'logs')
