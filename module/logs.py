# -*- coding:utf-8 -*-
import os
import datetime
from module.constant import LOG_FILE


#保留七天的日志，超过八天的删掉
def delLogs(filePath):
    if os.path.exists(filePath):
        logFileNames = os.listdir(filePath)
        if logFileNames!=[]:
            for logFileName in logFileNames:
                logDate=datetime.datetime.strptime(logFileName[-8:],'%Y%m%d')
                sysDate=datetime.datetime.now()
                if (sysDate-logDate).days>7:
                    # print(logFileName)
                    os.remove(os.path.join(filePath,logFileName))

#日志写入，在行首显示写入时间
def setLog(logInfo):
    sysDate=datetime.datetime.now()
    logName = 'log_{0}'.format(datetime.datetime.strftime(sysDate,'%Y%m%d'))
    # print(logName)
    logFile=os.path.join(LOG_FILE,logName)
    # print(logFile)
    formatInfo='{0}:  {1}'.format(datetime.datetime.strftime(sysDate,'%H:%M:%S.%f')[:-3],logInfo)
    # print(formatInfo)
    with open(logFile,'a') as f:
        f.write(formatInfo+'\n')



# folderName=createLogFolder('/home/python/VII')
#
# for i in  range(1,100):
#     info='{0}.{1}.{2}'.format(i,i,i)
#     setLog(folderName,info)
#
# delLogs(folderName)