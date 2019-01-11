# -*- coding:utf-8 -*-
import os
import module.logs as log
from module.constant import LOG_FILE, FILE_PATH

"""
只在程序启动时执行一次，路径暂时写死
如果需要可以改成传参
"""


# 创建存放日志文件的文件夹
def createLogFolder(filepath):
    if os.path.exists(filepath):
        name = os.path.join(filepath, 'logs')
        if os.path.exists(name) != True:
            os.makedirs(name)
    else:
        print("指定路径不存在")
    # return name


def initialize():
    createLogFolder(FILE_PATH)
    log.setLog("文件创建成功")
    # folderList=[logFolder,jsonFolder]
    #
    # return folderList
