# -*- coding:utf-8 -*-
import json
import MYSQL.MysqlDo as db
import time, traceback
from module.logs import setLog
from decimal import *


def getDictId(dictCode):
    dictId = None
    getDictSql = 'select id ' \
                 '  from stm_dict ' \
                 ' where dictcode = ' + '\'' + dictCode + '\'' + \
                 '  limit 1'
    results = db.select_table(getDictSql)
    if results:
        row = results[0]
        dictId = row[0]

    return dictId


# 保存配置信息 （有list的json，多表）
def saveCfgInfo(addr, data):
    try:
        succFlag = 0
        # 解析参数数据

        str = "INSERT INTO OP_MCH_DT_CFG(OP_MCH_HD_ID, GATHERDATE, CPUCOREQTY, DISKSIZE,SWAPSIZE,MEMSIZE, SYSTEMVERSION, EFFFLAG ,MEMO) " \
              "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (db.query_node_id(addr),
               # data['gatherDate'],
               time.strftime("%Y%m%d%H%M%S", time.strptime(data['gatherDate'], "%Y%m%d%H%M")),
               int(data['cpuCoreQty']),
               data['diskSize'],
               data['swapSize'],
               data['memSize'],
               data['systemVersion'],
               1,
               "")
        succFlag = db.insert_data(sql=str, val=val)

    except Exception:
        # logging.exception('保存配置信息 saveCfgInfo error')
        setLog('保存配置信息 saveCfgInfo error:{0}'.format(traceback.format_exc()))
        # setLog( '保存配置信息 saveCfgInfo.data:{0}'.format(data))
        succFlag = 0
    return succFlag


# 保存负载信息 （无list的json，单表）
def saveLoadInfo(addr, data):
    try:
        alarm = {}
        alarmList = []
        mchAlarmCode = 'D_HOST_NORMAL'
        # 解析参数数据
        infoSql = "INSERT INTO OP_MCH_DT_INFO(" \
                  "OP_MCH_HD_ID,GATHERDATE,CPUUS,CPUSY,CPUID,CPUWA,MEMUSED,MEMBUFFCACHE,SWAPUSED" \
                  ",LOADAVGFIVE,LOADAVGTEN,LOADAVGFIFTEEN,TOTALDISKREAD,TOTALDISKWRITE,NETINCOMINGAVG,NETOUTGOINGAVG,EFFFLAG,MEMO) " \
                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        # 保留三位小数
        infoVal = (db.query_node_id(addr),
                   # data['gatherDate'],
                   time.strftime("%Y%m%d%H%M%S", time.strptime(data['gatherDate'], "%Y%m%d%H%M")),
                   data['cpuUs'],
                   data['cpuSy'],
                   data['cpuId'],
                   data['cpuWa'],
                   float('%.3f' % data['memUsed']),
                   float('%.3f' % data['memBuffCache']),
                   float('%.3f' % data['swapUsed']),
                   data['loadAvgFive'],
                   data['loadAvgTen'],
                   data['loadAvgFifteen'],
                   float('%.3f' % data['totalDiskRead']),
                   float('%.3f' % data['totalDiskWrite']),
                   float('%.3f' % data['netIncomingAvg']),
                   float('%.3f' % data['netOutgoingAvg']),
                   1,
                   "")
        dirSql = "INSERT INTO OP_MCH_DT_DIR( DIRNAME, USEDSIZE,FILESYSTEM,DIRSIZE,USEDPER,MCH_ID, EFFFLAG ,MEMO,AMDLOG,AMDUSR,CRTLOG,CRTUSR) " \
                 "VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"

        succFlag = db.insert_data(infoSql, infoVal)
        if succFlag == 1:

            if len(data['dirInfo']) > 0:
                # 清除旧的数据
                deletesql = 'delete from op_mch_dt_dir where mch_id =' + str(db.query_node_id(addr))
                succFlag = db.delete_data(deletesql)
                if succFlag == 1:
                    for i in data['dirInfo']:
                        val = tuple(i.values()) + (
                            db.query_node_id(addr), 1, "", time.strftime("%Y%m%d%H%M%S", time.localtime())
                            , 1, time.strftime("%Y%m%d%H%M%S", time.localtime()), 1)
                        succFlag = db.insert_data(dirSql, val)
            # 拼接主机配置查询语句
            getMchCfgSql = 'select ' \
                           '  c.disksize' \
                           ', c.memsize' \
                           ', c.swapsize ' \
                           '  from op_mch_dt_cfg c' \
                           ' where c.op_mch_hd_id = ' + str(db.query_node_id(addr)) + \
                           ' order by c.gatherdate desc' \
                           ' limit 1'
            getMchCfgResults = db.select_table(getMchCfgSql)

            if getMchCfgResults:
                cfgRow = getMchCfgResults[0]
                totalMem = cfgRow[1]
                totalSwap = cfgRow[2]
                # 拼接超负荷设置查询sql语句
                getMchSetSql = 'select  ' \
                               '  o.cpu_flag ' \
                               ', o.cpu_warning ' \
                               ', o.cpu_serious' \
                               ', o.memory_flag' \
                               ', o.memory_warning' \
                               ', o.memory_serious' \
                               ', o.file_sys_dir_flag' \
                               ', o.file_sys_dir_warning' \
                               ', o.file_sys_dir_serious' \
                               ', o.disk_io_flag' \
                               ', o.disk_io_warning' \
                               ', (select s.dictcode from  stm_dict s where s.id = o.disk_io_warning_unit) disk_io_warning_code' \
                               ', o.disk_io_serious' \
                               ', (select s.dictcode from  stm_dict s where s.id = o.disk_io_serious_unit) disk_io_serious_code' \
                               ', o.swap_flag' \
                               ', o.swap_warning' \
                               ', o.swap_serious' \
                               ', o.net_io_flag' \
                               ', o.net_io_warning' \
                               ', (select s.dictcode from  stm_dict s where s.id = o.net_io_warning_unit)  net_io_warning_unit_code' \
                               ', o.net_io_serious' \
                               ', (select s.dictcode from  stm_dict s where s.id = o.net_io_serious_unit)  net_io_serious_unit_code' \
                               ', o.load_flag' \
                               ', o.load_warning' \
                               ', o.load_serious' \
                               ', o.time_check_flag' \
                               ', o.time_check_num' \
                               ' from op_mch_set_info o ' \
                               'where o.effflag = 1 ' \
                               '  and o.mch_id =' + str(db.query_node_id(addr))
                getMchSetResults = db.select_table(getMchSetSql)

                if getMchSetResults:

                    row = getMchSetResults[0]
                    # 判断cpu是否超负载
                    if row[0] == 1:
                        alarm = {}
                        if Decimal.from_float(data['cpuUs']) >= row[2]:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = 'cpu使用率' + str(data['cpuUs'] * 100) + '%' + '负荷严重！'
                        elif Decimal.from_float(data['cpuUs']) >= row[1]:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = 'cpu使用率' + str(data['cpuUs'] * 100) + '%' + '负荷告警！'
                        else:
                            pass
                        if len(alarm) > 0:
                            alarmList.append(alarm)
                    # 判断内存是否超负载
                    if row[3] == 1:
                        if totalMem:
                            alarm = {}

                            if Decimal.from_float(data['memUsed']) / totalMem >= row[5]:
                                alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                                alarm['alarminfo'] = '内存使用达' + str(data['memUsed']) + 'M' + '使用严重！'
                            elif Decimal.from_float(data['memUsed']) / totalMem >= row[4]:
                                alarm['alarmtypecode'] = 'D_ALARM_WARM'
                                alarm['alarminfo'] = '内存使用达' + str(data['memUsed']) + 'M' + '使用告警！'
                            else:
                                pass

                            if len(alarm) > 0:
                                alarmList.append(alarm)
                    # 判断文件系统使用状况
                    if row[6] == 1:
                        if len(data['dirInfo']) > 0:
                            for i in data['dirInfo']:
                                alarm = {}
                                if Decimal.from_float(i['usedPer']) >= row[8]:
                                    alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                                    alarm['alarminfo'] = '文件系统 挂载点:' + i['dirName'] + '  ' + str(
                                        i['usedPer'] * 100) + '%' + '使用严重！'
                                elif Decimal.from_float(i['usedPer']) >= row[7]:
                                    alarm['alarmtypecode'] = 'D_ALARM_WARM'
                                    alarm['alarminfo'] = '文件系统 挂载点:' + i['dirName'] + '  ' + str(
                                        i['usedPer'] * 100) + '%' + '使用告警！'
                                else:
                                    pass

                        if len(alarm) > 0:
                            alarmList.append(alarm)

                    # 判断磁盘io负载
                    if row[9] == 1:

                        # 更新警告阀值为kb
                        warnIo = row[10]
                        if row[11] == 'D_IOUNIT_MB':
                            warnIo = row[10] * 1024
                        elif row[11] == 'D_IOUNIT_GB':
                            warnIo = row[10] * 1024 * 1024
                        else:
                            pass
                        # 更新危险阀值为kb
                        serIo = row[12]
                        if row[13] == 'D_IOUNIT_MB':
                            serIo = row[12] * 1024
                        elif row[13] == 'D_IOUNIT_GB':
                            serIo = row[12] * 1024 * 1024
                        else:
                            pass
                        # 重置告警信息
                        alarm = {}
                        if Decimal.from_float(data['totalDiskRead']) >= serIo:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '磁盘读取io 为' + str(data['totalDiskRead']) + 'kb/s' + '负载严重！'
                        elif Decimal.from_float(data['totalDiskRead']) >= warnIo:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '磁盘读取io 为' + str(data['totalDiskRead']) + 'kb/s' + '负载告警！'

                        if len(alarm) > 0:
                            alarmList.append(alarm)
                        # 重置告警信息
                        alarm = {}
                        if Decimal.from_float(data['totalDiskWrite']) >= serIo:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '磁盘写入io 为' + str(data['totalDiskWrite']) + 'kb/s' + '负载严重！'
                        elif Decimal.from_float(data['totalDiskWrite']) >= warnIo:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '磁盘写入io 为' + str(data['totalDiskWrite']) + 'kb/s' + '负载告警！'

                        if len(alarm) > 0:
                            alarmList.append(alarm)
                    # 判断Swap是否超负载
                    if row[14] == 1:
                        if totalSwap:
                            # 重置告警信息
                            alarm = {}
                            if Decimal.from_float(data['swapUsed']) / totalSwap >= row[16]:
                                alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                                alarm['alarminfo'] = 'Swap使用达为' + str(data['swapUsed']) + 'M' + '使用严重！'
                            elif Decimal.from_float(data['swapUsed']) / totalSwap >= row[15]:
                                alarm['alarmtypecode'] = 'D_ALARM_WARM'
                                alarm['alarminfo'] = 'Swap使用达为' + str(data['swapUsed']) + 'M' + '使用告警！'
                            else:
                                pass

                            if len(alarm) > 0:
                                alarmList.append(alarm)
                    # 判断网络io负载
                    if row[17] == 1:

                        # 更新警告阀值为kb
                        warnnetIo = row[18]
                        if row[19] == 'D_IOUNIT_MB':
                            warnnetIo = row[18] * 1024
                        elif row[19] == 'D_IOUNIT_GB':
                            warnnetIo = row[18] * 1024 * 1024
                        else:
                            pass
                        # 更新危险阀值为kb
                        sernetIo = row[20]
                        if row[21] == 'D_IOUNIT_MB':
                            sernetIo = row[20] * 1024
                        elif row[21] == 'D_IOUNIT_GB':
                            sernetIo = row[20] * 1024 * 1024
                        else:
                            pass
                        # 重置告警信息
                        alarm = {}
                        if Decimal.from_float(data['netIncomingAvg']) >= sernetIo:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '网络平均下载io 为' + str(data['netIncomingAvg']) + 'kb/s' + '负载严重！'
                        elif Decimal.from_float(data['totalDiskRead']) >= warnnetIo:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '网络平均下载io 为' + str(data['netIncomingAvg']) + 'kb/s' + '负载告警！'

                        if len(alarm) > 0:
                            alarmList.append(alarm)
                        # 重置告警信息
                        alarm = {}
                        if Decimal.from_float(data['netOutgoingAvg']) >= sernetIo:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '网络平均上传io 为' + str(data['netOutgoingAvg']) + 'kb/s' + '负载严重！'
                        elif Decimal.from_float(data['totalDiskWrite']) >= warnnetIo:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '网络平均上传io 为' + str(data['netOutgoingAvg']) + 'kb/s' + '负载告警！'

                        if len(alarm) > 0:
                            alarmList.append(alarm)
                    # 判断负载均衡数值
                    if row[22] == 1:
                        alarm = {}
                        if Decimal.from_float(data['loadAvgFive']) >= row[24]:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '五分钟负载均衡达' + str(data['loadAvgFive']) + '负荷严重！'
                        elif Decimal.from_float(data['loadAvgFive']) >= row[23]:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '五分钟负载均衡达' + str(data['loadAvgFive']) + '负荷告警！'
                        else:
                            pass
                        if len(alarm) > 0:
                            alarmList.append(alarm)
                        alarm = {}
                        if Decimal.from_float(data['loadAvgTen']) >= row[24]:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '十分钟分钟负载均衡达' + str(data['loadAvgTen']) + '负荷严重！'
                        elif Decimal.from_float(data['loadAvgFive']) >= row[23]:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '十分钟负载均衡达' + str(data['loadAvgTen']) + '负荷告警！'
                        else:
                            pass
                        if len(alarm) > 0:
                            alarmList.append(alarm)
                        alarm = {}
                        if Decimal.from_float(data['loadAvgFifteen']) >= row[24]:
                            alarm['alarmtypecode'] = 'D_ALARM_DANGER'
                            alarm['alarminfo'] = '十五分钟负载均衡达' + str(data['loadAvgFifteen']) + '负荷严重！'
                        elif Decimal.from_float(data['loadAvgFive']) >= row[23]:
                            alarm['alarmtypecode'] = 'D_ALARM_WARM'
                            alarm['alarminfo'] = '十五分钟负载均衡达' + str(data['loadAvgFifteen']) + '负荷告警！'
                        else:
                            pass
                        if len(alarm) > 0:
                            alarmList.append(alarm)
                # 写入报警信息表
                if alarmList:
                    # 存在告警信息代表主机状态至少是警告状态
                    mchAlarmCode = 'D_HOST_WARNING'
                    for alarm in alarmList:
                        alarm_type_id = getDictId(alarm['alarmtypecode'])
                        alarm_info = alarm['alarminfo']
                        # 拼接select语句
                        alarmSql = "INSERT INTO op_mch_dt_alarm(" \
                                   "OP_MCH_HD_ID,GATHERDATE,ALARM_TYPE_ID,ALARM_INFO,EFFFLAG) "

                        # 拼接value语句
                        alarmVal = 'values' + '(' \
                                   + str(db.query_node_id(addr)) + ',' \
                                   + '\'' + time.strftime("%Y%m%d%H%M%S",
                                                          time.strptime(data['gatherDate'], "%Y%m%d%H%M")) + '\'' + ',' \
                                   + ('null' if not alarm_type_id else str(alarm_type_id)) + ',' \
                                   + '\'' + alarm_info + '\'' + ',' \
                                   + str(1) + ')'

                        if alarmSql:
                            succFlag = db.insert_data_one(alarmSql + alarmVal)

                        # 如果存在危险信息 则更新主机状态为危险
                        if alarm['alarmtypecode'] == 'D_ALARM_DANGER' and mchAlarmCode != 'D_HOST_ERROR':
                            mchAlarmCode = 'D_HOST_ERROR'
                else:
                    # 无告警信息代表正常
                    mchAlarmCode = 'D_HOST_NORMAL'

                updateMchSql = 'update op_mch_hd h ' \
                               '   set h.mch_status_id = ' + str(getDictId(mchAlarmCode)) + \
                               ' where h.id =' + str(db.query_node_id(addr))
                if updateMchSql:
                    db.update_table(updateMchSql)


    except Exception as e:
        setLog('保存负载信息 saveLoadInfo error:{0}'.format(traceback.format_exc()))
        # setLog( '保存配置信息 saveCfgInfo.data:{0}'.format(data))
        succFlag = 0
    return succFlag


# 保存服务进程信息
def saveSeverInfo(addr, data):
    try:
        succFlag = 1
        mchhdid = db.query_node_id(addr)
        serverList = data['serverList']
        updatesql = None
        updateflag = 0

        # 拼接超负荷设置查询sql语句
        getMchSetSql = 'select  ' \
                       '  o.server_flag ' \
                       ' from op_mch_set_info o ' \
                       'where o.effflag = 1 ' \
                       '  and o.mch_id =' + str(db.query_node_id(addr))
        getMchSetResults = db.select_table(getMchSetSql)
        if getMchSetResults:
            # 获取第一条信息
            setRow = getMchSetResults[0]
            # 判断是否需要写入进程
            if setRow[0] == 1:
                getsql = 'select ds.id, d.dictname,ds.last_begin_time,ds.last_close_time,ds.pid ' \
                         '  from op_mch_dt_server s' \
                         '      ,op_mch_dtt_server ds ' \
                         '      ,stm_dict d ' \
                         ' where s.id = ds.op_mch_dt_server_id ' \
                         '   and ds.node_id = d.id ' \
                         '   and  s.op_mch_hd_id =' + str(mchhdid)
                results = db.select_table(getsql)
                for row in results:
                    nodeName = row[1]
                    for server in serverList:
                        if server['nodeName'] == nodeName:
                            # 无开始时间为第一次获取 更新开始时间
                            if not row[2]:
                                updatesql = 'update op_mch_dtt_server ds ' \
                                            '   set ds.last_begin_time =' + 'str_to_date' + '(' + data[
                                                'gatherDate'] + ',' + '\'%Y%m%d%H%i\')' + \
                                            '      ,ds.pid = ' + server['pid'] + \
                                            ' where ds.id =' + str(row[0])

                                # 之前已有结束时间则认为已结束 此次检测到则重置开始时间 和 结束时间
                            elif row[3]:
                                updatesql = 'update op_mch_dtt_server ds  ' \
                                            '   set ds.last_begin_time =' + 'str_to_date' + '(' + data[
                                                'gatherDate'] + ',' + '\'%Y%m%d%H%i\')' \
                                                                      '      ,ds.last_close_time =' + 'null' \
                                                                                                      ' where ds.id =' + str(
                                    row[0])
                            updateflag = 1
                        else:
                            pass

                    # 如果数据表有数据 但是轮询采集的进程无匹配数据 则认为进程已关闭 且当前结束时间为空 开始更新结束时间
                    if updateflag == 0 and not row[3]:
                        updatesql = 'update op_mch_dtt_server ds  ' \
                                    '   set ds.last_close_time =' + 'str_to_date' + '(' + data[
                                        'gatherDate'] + ',' + '\'%Y%m%d%H%i\')' \
                                                              '      ,ds.pid = null' \
                                                              ' where ds.id =' + str(row[0])

                    if updatesql:
                        succFlag = db.update_table(updatesql)

    except Exception as e:
        succFlag = 0
        results = []
        # logging.exception('保存服务进程信息 saveSeverInfo error')
        setLog('保存服务进程信息 saveSeverInfo error:{0}'.format(traceback.format_exc()))
        # setLog( '保存服务进程信息 saveSeverInfo.data:{0}'.format(data))
    return succFlag


# 数据处理路由
def datarouter(addr, data):
    # # 可以定义数据里的前几个字节为路由数据
    js = json.loads(data)

    interfaceCode = js['interfaceCode']

    mchhdid = db.query_node_id(addr)
    if mchhdid:
        if interfaceCode == 'cfgInfo':
            # logging.info('do something :cfiinfo ')
            return saveCfgInfo(addr, js)
        elif interfaceCode == 'loadInfo':
            # logging.info('do something :loadinfo ')
            return saveLoadInfo(addr, js)
        elif interfaceCode == 'serverInfo':

            return saveSeverInfo(addr, js)

    else:
        # logging.info('do something others')
        return 0
