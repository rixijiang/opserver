#!/usr/bin/python
# -*- coding:utf-8 -*-

# 导入socket/sys模块
import threading
import socket
import logging
import traceback
import time
from module.socketdo import RecvData
from module.constant import CON_RECV, CON_SEND
from module.initialization import initialize
from module.logs import setLog
from module.testConnection import testConnect, monitor
from module.socketdo import SendData

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 定义了一个套接字
serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# 绑定地址
serverSocket.bind(CON_RECV['ADDR'])
# 设置最大连接数据，超过后排队
serverSocket.listen(5)


# 执行间隔时间
def sleepTime(hour, min, sec):
    return hour * 3600 + min * 60 + sec


class createThread(threading.Thread):
    def __init__(self, threadID, name, sleeptime, target, args):
        super(createThread, self).__init__(target=target, args=args)
        self.threadID = threadID
        self.name = name
        self.sleeptime = sleeptime

    def run(self):
        # print("开始线程：" + self.name)
        super(createThread, self).run()
        # print("退出线程：" + self.name)


# #监控代理是否正常执行
# def monitor():
#     bufferStr=111
#     srecvmsg = SendData(CON_SEND['ADDR'], CON_SEND['BUFFSIZE'], bufferStr)


# 数据接收线程
def receiveData():
    print('进入进程')
    while True:
        print('receiveData')
        try:
            # print('Waiting for connection...')
            clientSocket, addr = serverSocket.accept()

            print('addr =', addr)
            RecvData(clientSocket, addr, CON_RECV['BUFFSIZE'])
        except Exception as e:
            setLog('数据接收失败:{0}'.format(e))
            exit()


# 测试连接线程
def testConn(sleeptime):
    while True:
        print('testConn')
        try:
            # print('Waiting for connection...')
            testConnect()
        except Exception as e:
            setLog('连接测试失败:{0}'.format(e))
            exit()
        time.sleep(sleeptime)


# 测试连接线程
def agentMonitor(sleeptime):
    while True:
        print('agentMonitor')
        try:
            # print('Waiting for connection...')
            monitor()
        except Exception as e:
            setLog('代理进程监控失败:{0}'.format(e))
            exit()
        time.sleep(sleeptime)


if __name__ == '__main__':
    try:
        # 只在服务启动的时候执行一次，创建日志文件
        initialize()

        connSleepTime = sleepTime(0, 1, 0)
        monitorSleepTime = sleepTime(0, 10, 0)
        # 启用多线程处理消息接收
        receiveData = createThread(1, "receiveData ", '', receiveData, (''))
        testconn = createThread(2, "testConn ", connSleepTime, testConn, (connSleepTime,))
        agentmonitor = createThread(3, "agentMonitor ", monitorSleepTime, agentMonitor, (monitorSleepTime,))
        receiveData.start()
        testconn.start()
        agentmonitor.start()
        receiveData.join()
        testconn.join()
        agentmonitor.join()
    except Exception as e:
        # print('traceback.format_exc()=', traceback.format_exc())
        setLog(traceback.format_exc())
        serverSocket.close()
        raise
