#!/usr/bin/python
# -*- coding:utf-8 -*-

import pymysql
from module.constant import MYSQL_DATABASE,MCH_REC_PORT
from module.logs import setLog
import subprocess
import os, time, json
import traceback
import threading
from math import ceil
from module.socketdo import SendData
import MYSQL.MysqlDo as db


class createThread(threading.Thread):
    def __init__(self, threadID, name, target, args):
        super(createThread, self).__init__(target=target, args=args)
        self.threadID = threadID
        self.name = name

    def run(self):
        super(createThread, self).run()


# 临时修改系统环境变量
os.environ['NLS_LANG'] = 'AMERICAN_AMERICA.UTF8'


# 连接运维平台的mysql数据库
def connect_op_db():
    return pymysql.connect(host=MYSQL_DATABASE['host'],
                           port=MYSQL_DATABASE['port'],
                           # charset='latin1',
                           user=MYSQL_DATABASE['user'],
                           password=MYSQL_DATABASE['password'],
                           database=MYSQL_DATABASE['db'])


def connMch(mchList):
    try:
        con = connect_op_db()
        cur = con.cursor()
        selectDict = ("SELECT id,dictcode from stm_dict where dictcode in ('D_HOST_NORMAL','D_HOST_CLOSE') ")
        cur.execute(selectDict)
        dictRow = cur.fetchall()
        for dictInfo in dictRow:
            if dictInfo[1] == 'D_HOST_NORMAL':
                normalKey = dictInfo[0]
            elif dictInfo[1] == 'D_HOST_CLOSE':
                closeKey = dictInfo[0]
        for info in mchList:
            mchId = info[0]
            ipAddr = info[1]
            result = subprocess.call('ping -c 1 ' + ipAddr, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            modifyTime = time.strftime("%Y%m%d%H%M%S", time.localtime())

            if result == 1:
                if not info[2]:
                    sqlUpdateStr = "update OP_MCH_HD set break_time={0},mch_status_id={1},run_time=NULL,amdlog={0}  where id={2}".format(
                        modifyTime, closeKey, mchId)
                    cur.execute(sqlUpdateStr)
            else:
                if not info[3]:
                    sqlUpdateStr = "update OP_MCH_HD set run_time={0},mch_status_id={1},break_time=NULL,amdlog={0}  where id={2}".format(
                        modifyTime, normalKey, mchId)
                    cur.execute(sqlUpdateStr)
            con.commit()

    except:
        raise
        setLog('主机测试连接失败：{0}'.format(traceback.format_exc()))
    finally:
        cur.close()
        con.close()


# 监控代理是否正常执行
def monitor():
    try:
        sqlStr = ("SELECT H.id,H.ipaddr,h.break_time,h.run_time"
                  + " FROM OP_MCH_HD H,OP_MCH_SET_INFO I WHERE I.mch_id=H.id AND I.agent_process_flag=1")
        # sqlStr = ("SELECT H.id,H.ipaddr"
        #           + " FROM OP_MCH_HD H")
        con = connect_op_db()
        cur = con.cursor()
        cur.execute(sqlStr)
        row = cur.fetchall()
        for mchInfo in row:
            mchId = mchInfo[0]
            bufferStr = '{0}{1}'.format(mchId, time.strftime("%Y%m%d%H%M%S", time.localtime()))
            ipAddr = (mchInfo[1], MCH_REC_PORT)
            srecvmsg = json.loads(SendData(ipAddr, 1024, bufferStr))
            curTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
            if int(srecvmsg['code']) != 200:
                sqlUpdateStr = "INSERT INTO  OP_MCH_DT_AGENT( OP_MCH_HD_ID, AGENT_NAME, END_TIME,MEMO, CRTUSR, CRTLOG ,AMDUSR,AMDLOG) " \
                               'VALUES (' + str(
                    mchId) + ',' + '\'' + '数据采集代理程序' + '\'' + ',' + curTime + ',' + '\'' + '数据采集代理程序失败' + '\'' + ',' + '1' + ',' + curTime + ',' + '1' + ',' + curTime + ')'
                succFlag = db.insert_data_one(sqlUpdateStr)
                if succFlag != 1:
                    setLog('主机代理进程监控失败：{0}'.format(traceback.format_exc()))
            # else:
            #     sqlUpdateStr = "INSERT INTO  OP_MCH_DT_AGENT( OP_MCH_HD_ID, AGENT_NAME, END_TIME,MEMO, CRTUSR, CRTLOG ,AMDUSR,AMDLOG) " \
            #                    'VALUES (' + str(
            #         mchId) + ',' + '\'' + '数据采集代理程序' + '\'' + ',' + curTime + ',' + '\'' + '数据采集代理程序' + '\'' + ',' + '1' + ',' + curTime + ',' + '1' + ',' + curTime + ')'
            #     succFlag = db.insert_data_one(sqlUpdateStr)
    except:
        # raise
        setLog('主机代理进程监控失败：{0}'.format(traceback.format_exc()))
    finally:
        cur.close()
        con.close()


def testConnect():
    # startTime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    sqlStr = ("SELECT H.id,H.ipaddr,break_time,run_time"
              + " FROM OP_MCH_HD H,OP_MCH_SET_INFO I WHERE I.mch_id=H.id AND I.normal_flag=1")
    # logging.info(sql_str)

    con = connect_op_db()
    cur = con.cursor()
    cur.execute(sqlStr)
    row = cur.fetchall()
    mchList = []
    start = 0
    for i in range(0, 7):
        end = start + ceil(len(row) / 7)
        mchList.append(row[start:end])
        start = end

    try:
        thread1 = createThread(1, "thread1 ", connMch, (mchList[0],))
        thread2 = createThread(2, "thread2 ", connMch, (mchList[1],))
        thread3 = createThread(3, "thread3 ", connMch, (mchList[2],))
        thread4 = createThread(4, "thread4 ", connMch, (mchList[3],))
        thread5 = createThread(5, "thread5 ", connMch, (mchList[4],))
        thread6 = createThread(6, "thread6 ", connMch, (mchList[5],))
        thread7 = createThread(7, "thread7 ", connMch, (mchList[6],))
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()
        thread5.start()
        thread6.start()
        thread7.start()
        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
        thread5.join()
        thread6.join()
        thread7.join()
    except:
        # raise
        setLog('多线程主机连接测试失败：{0}'.format(traceback.format_exc()))
    finally:
        cur.close()
        con.close()
