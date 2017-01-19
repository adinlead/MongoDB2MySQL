# coding=utf8
import sys
import ConfigParser

import math
import threading

from tqdm import tqdm

from Tools import *
from DBUtils import *

def disposeData(data,conn):
    key_arr = '_id'
    pla_arr = '%s'
    val_arr = [str(data['_id']).upper()]
    for key in data.keys():
        son = data[key]
        if '_id' == key:
            continue
        if 'show' == key:
            continue
        if isinstance(son, dict):
            ret = ergodic.ergodicDict(data[key], prefix='%s_' % key)
            key_arr += ret['key']
            pla_arr += ret['pla']
            val_arr.extend(ret['val'])
            list_obj = ret['list']
            for son_key in list_obj.keys():
                intermediate = '%s_%s_%s_intermediate' % (mongodb_collection, key, son_key)
                intermediate_key_arr = 'tractor,trailer'
                intermediate_pla_arr = '%s,%s'
                intermediate_list = []
                subList = list_obj[son_key]
                for sub in subList:
                    href = sub['href']
                    id = href[href.rfind('/') + 1:]
                    ret = ergodic.ergodicDict(sub, son_key + "_")
                    son_key_arr = ret['key']
                    son_key_arr = '`id`' + son_key_arr
                    son_pla_arr = ret['pla']
                    son_pla_arr = '%s' + son_pla_arr
                    son_val_arr = ret['val']
                    son_val_arr.insert(0, id)
                    intermediate_list.append([[str(data['_id']).upper()], id])
                    conn.executeInsterSQL(son_key, son_key_arr, son_pla_arr, son_val_arr)
                conn.executeInsterSQLOfMultiterm(intermediate, intermediate_key_arr, intermediate_pla_arr,
                                                 intermediate_list)
        elif isinstance(son, list):
            pass
        elif isinstance(son, str):
            key_arr += (',`%s`' % key)
            pla_arr += ',%s'
            val_arr.append(son)
        elif isinstance(son, unicode):
            key_arr += (',`%s`' % key)
            pla_arr += ',%s'
            val_arr.append(unicode(son))
        else:
            key_arr += (',`%s`' % key)
            pla_arr += ',%s'
            val_arr.append(unicode(str(son)))
    conn.executeInsterSQL(mongodb_collection, key_arr, pla_arr, val_arr)

if __name__ == '__main__':
    args = sys.argv
    del args[0]

    mongodb_uri = None
    mongodb_port = None
    mongodb_db = None
    mongodb_collection = None

    mysql_host = None
    mysql_port = None
    mysql_user = None
    mysql_passwd = None
    mysql_db = None
    mysql_table = None

    # 遇到列表则创建新的表
    list_to_new_table = True

    config_name = 'config.ini'

    if len(args) > 0 and 'config=' in args[0]:
        config_name = args[0].replace('config=', '')

    config = ConfigParser.ConfigParser()
    config.readfp(open(config_name, "rb"))
    mongodb_uri = config.get("db", "mongodb_uri")
    mongodb_port = config.get("db", "mongodb_port")
    mongodb_port = int(mongodb_port)
    mongodb_db = config.get("db", "mongodb_db")
    mongodb_collection = config.get("db", "mongodb_collection")

    mysql_host = config.get("db", "mysql_host")
    mysql_port = config.get("db", "mysql_port")
    mysql_port = int(mysql_port)
    mysql_user = config.get("db", "mysql_user")
    mysql_passwd = config.get("db", "mysql_passwd")
    mysql_db = config.get("db", "mysql_db")
    mysql_table = config.get("db", "mysql_table")

    list_to_new_table = config.get('ot', 'list_to_new_table')

    ergodic = Ergodic()

    conn = Holder()
    conn.mysql_db = mysql_db
    conn.collection = mongodb_collection
    conn.initMongoDB(mongodb_uri, mongodb_port, mongodb_db)
    conn.initMySql(mysql_host, mysql_port, mysql_user, mysql_passwd, mysql_db)

    limit = 100
    mongo_count = conn.countMongoDB()
    mongo_all_page = int(math.ceil(mongo_count / limit))

    meter = tqdm(initial=0,total=mongo_count)
    for page in range(0, mongo_all_page + 1):
        mongo_data_list = conn.readMongoTable(page, limit)
        for data in mongo_data_list:
            disposeData(data,conn)
            # threading.Thread(target=disposeData, args=(data,conn)).start()
            meter.update(1)