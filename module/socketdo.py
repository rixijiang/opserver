# -*- coding:utf-8 -*-
import socket, logging, re, time, traceback
from module.logs import setLog
from module.constant import LOG_FILE
from data.datado import datarouter


# TCP/IP 消息发送
def SendData(addr, buffersize, buffer):
    try:
        hostname = '{' + socket.gethostname() + '}'

        # 定义一个套接字
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # 连接服务，指定主机和端口
        sock.connect(addr)
        sock.recv(buffersize).decode('utf-8')
        # buffer最前面10位加上信息内容长度，10到23位为发送的时间戳（毫秒级）
        buffer = str('%010d' % (len(buffer) + 23 + len(hostname))) + str(
            int(round(time.time() * 1000))) + hostname + buffer
        sock.send(buffer.encode('utf-8'))
        srecvmsg = sock.recv(buffersize).decode('utf-8')
        # 日志直接写返回json
        logging.info('recvtime:' + srecvmsg + ',data:' + buffer[23 + len(hostname):])
        sock.close()
    except Exception as e:
        setLog('信息发送失败:{0}'.format(traceback.format_exc()))
        srecvmsg = '{0}"code":999,"msg":"发送失败{1}"{2}'.format('{', e, '}')
    return srecvmsg


# TCP/IP消息的接收
def RecvData(sock, addr, buffsize):
    try:
        data = ''
        sock.send(b'Welcome!')
        # 用字符串数组缓存数据
        buffer = []
        while True:
            # 一次接收1024字节的数据
            try:
                recv = sock.recv(buffsize).decode('utf-8')
            except Exception as e:
                logging.INFO(e)
            # 休眠1s
            time.sleep(0.1)
            # 如果还能接收到数据，则继续接收，否则中断接收
            if recv:
                buffer.append(recv)
            else:
                break
            # 字符数组组合成字符串
            data = data + ''.join(buffer)
            buffer = []
            # data最前面10位为信息内容长度，10到23位为发送的时间戳（毫秒级），后面的为实际内容
            # 以上内容Demo格式，具体格式需要统一定义
            if len(data) >= int(data[0:10]):
                # lens = data[0:10]
                sendtime = data[10:23]
                hostname = re.findall(r'({.*?})', data[23:])[0]
                data = data[23 + len(hostname):]
                break

        # print('lens：' + str(int(lens)))  # 信息内容长度
        # print('sendtime：' + sendtime)    # 发送的时间戳（毫秒级）
        # print('hostname:' + hostname)     # hostname
        # print('data:' + data)             # 实际内容

        # 调用数据处理路由，处理数据
        # datarouter(data)
        # sock.close()
        lens = str(len(data))
        if datarouter(addr[0], data):
            # add by cwt 20181022 返回接收数据后的处理数据结果
            # 如果成功则，code=200
            srecvmsg = '{"code":"200","lens":"' + lens + '","msg":"传输成功"}'
            sock.send(srecvmsg.encode('utf-8'))
        else:
            srecvmsg = '{"code":"998","lens":"' + lens + '","msg":"数据库数据写入失败"}'
            sock.send(srecvmsg.encode('utf-8'))
    except Exception as e:
        srecvmsg = '{"code":"999","lens":"' + lens + '","msg":"{0}"{1}'.format(e, '}')
        sock.send(srecvmsg.encode('utf-8'))
        setLog(traceback.format_exc())
    finally:
        sock.close()
