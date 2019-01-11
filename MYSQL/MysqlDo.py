# -*- coding:utf-8 -*-
import pymysql, traceback
from module.constant import MYSQL_DATABASE
from module.logs import setLog


# 连接运维平台的mysql数据库
def connect_op_db():
    return pymysql.connect(host=MYSQL_DATABASE['host'],
                           port=MYSQL_DATABASE['port'],
                           # charset='latin1',
                           user=MYSQL_DATABASE['user'],
                           password=MYSQL_DATABASE['password'],
                           database=MYSQL_DATABASE['db'])


# 查询节点ID
def query_node_id(ipAddr):
    sql_str = ("SELECT id"
               + " FROM OP_MCH_HD"
               + " WHERE ipAddr='%s'" % (ipAddr))
    # logging.info(sql_str)

    con = connect_op_db()
    cur = con.cursor()
    cur.execute(sql_str)
    row = cur.fetchone()
    cur.close()
    con.close()
    if not (row):
        setLog('Fatal error: ipAddr:{0} does not exists!'.format(ipAddr))
    else:
        return row[0]


#  简单单条插入
def insert_data1(sql, ):
    con = connect_op_db()
    cur = con.cursor()
    succFlag = 0
    try:
        cur.execute(sql)
        # lastrowid = cur.lastrowid
        con.commit()
        succFlag = 1
    except Exception:
        con.rollback()
        setLog('insert_data() Insert operation error:{0}'.format(traceback.format_exc()))
        raise
        succFlag = 0
    finally:
        cur.close()
        con.close()
        return succFlag


#  简单单条插入
def insert_data(sql, val):
    con = connect_op_db()
    cur = con.cursor()
    succFlag = 0
    try:
        cur.execute(sql, val)
        # lastrowid = cur.lastrowid
        con.commit()
        succFlag = 1
    except Exception:
        con.rollback()
        setLog('insert_data() Insert operation error:{0}'.format(traceback.format_exc()))
        raise
        succFlag = 0
    finally:
        cur.close()
        con.close()
        return succFlag


# 批量插入
def insert_data_batch(sql, val):
    con = connect_op_db()
    cur = con.cursor()
    succFlag = 0
    try:
        cur.executemany(sql, val)
        assert cur.rowcount == len(val), 'my error message'
        con.commit()
        succFlag = 1
    except Exception:
        con.rollback()
        setLog('insert_data_batch() Insert operation error:{0}'.format(traceback.format_exc()))
        succFlag = 0
    finally:
        cur.close()
        con.close()
        return succFlag


# 多表插入
def insert_table_batch(hdSql, hdVal, dtSql, dtValList):
    con = connect_op_db()
    cur = con.cursor()
    valList = []
    succFlag = 0
    try:
        cur.execute(hdSql, hdVal)
        hdKey = cur.lastrowid
        for i in dtValList:
            i = (hdKey,) + i
            valList.append(i)
        cur.executemany(dtSql, valList)
        assert cur.rowcount == len(dtValList), 'my error message'
        con.commit()
        succFlag = 1
    except Exception:
        setLog('insert_table_batch() Insert operation error:{0}'.format(traceback.format_exc()))
        con.rollback()
        succFlag = 0
        # logging.exception('Insert operation error')
        return 0
    finally:
        cur.close()
        con.close()
        return succFlag


# 表查询
def select_table(sql):
    con = connect_op_db()
    cur = con.cursor()
    try:
        cur.execute(sql)
        results = cur.fetchall()
    except Exception:
        setLog('select_table() select operation error:{0}'.format(traceback.format_exc()))
        con.rollback()
        return 0
    finally:
        cur.close()
        con.close()
        return results


#  删除表数据
def delete_data(sql):
    con = connect_op_db()
    cur = con.cursor()
    succFlag = 1
    try:
        cur.execute(sql)
        con.commit()
    except Exception:
        succFlag = 0
        setLog('delete_data() delete operation error:{0}'.format(traceback.format_exc()))
        con.rollback()
    finally:
        cur.close()
        con.close()
        return succFlag



#  更新表字段
def update_table(sql):
    con = connect_op_db()
    cur = con.cursor()
    succFlag = 1
    try:
        cur.execute(sql)
        con.commit()
    except Exception:
        succFlag = 0
        setLog('update_table() update operation error:{0}'.format(traceback.format_exc()))
        con.rollback()
    finally:
        cur.close()
        con.close()
        return succFlag

#  单条插入
def insert_data_one(sql):
    con = connect_op_db()
    cur = con.cursor()
    succFlag = 0
    try:
        cur.execute(sql)
        # lastrowid = cur.lastrowid
        con.commit()
        succFlag = 1
    except Exception:
        con.rollback()
        setLog('insert_data_one() Insert operation error:{0}'.format(traceback.format_exc()))
        raise
        succFlag = 0
    finally:
        cur.close()
        con.close()
        return succFlag



