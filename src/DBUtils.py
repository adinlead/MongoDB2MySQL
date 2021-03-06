# coding=utf8
from pandas import json

import MySQLdb
from pymongo import MongoClient


class MongoHolder():
    mongodb = None
    collection = None

    def initMongoDB(self, uri, port, dbname):
        client = MongoClient(uri, port, maxPoolSize=200, connectTimeoutMS=60 * 1000, socketTimeoutMS=60 * 1000)
        self.mongodb = client[dbname]

    def readMongoTable(self, page, limit):
        return self.mongodb[self.collection].find().skip(page * limit).limit(limit)

    def countMongoDB(self):
        return self.mongodb[self.collection].count()


class MySQLHolder():
    mysql = None
    collection = None
    mysql_db = None  # mySQL数据库名称
    text_column = []
    unique_column = []

    def initMySql(self, host, port, user, passwd, dbname):
        self.mysql = MySQLdb.connect(
            host=host,
            port=port,
            user=user,
            passwd=passwd,
            db=dbname,
            charset="utf8"
        )

    def createMySqlTable(self, tableName):
        base_sql = 'CREATE TABLE `%s` (`_idx_` INT NOT NULL AUTO_INCREMENT,PRIMARY KEY (`_idx_`))DEFAULT CHARSET=utf8'
        cursor = self.mysql.cursor()
        cursor.execute(base_sql % tableName)
        data = cursor.fetchone()
        return data

    def createMySqlFieldToTable(self, tableName, fieldName, fieldType, default='', unique=False):
        try:
            if unique:
                sql = 'ALTER TABLE `%s` ADD COLUMN `%s` %s %s,' \
                      'ADD UNIQUE INDEX `%s_UNIQUE` (`%s` ASC)' % (
                          tableName, fieldName, fieldType, default, fieldName, fieldName)
            else:
                sql = 'ALTER TABLE `%s` ADD COLUMN `%s` %s %s;' % (tableName, fieldName, fieldType, default)
            cursor = self.mysql.cursor()
            cursor.execute(sql)
            data = cursor.fetchone()
            return data
        except Exception, e:
            pass

    def executeSQL(self, sql):
        cursor = self.mysql.cursor()
        cursor.execute(sql)
        data = cursor.fetchone()
        return data

    def executeSQL(self, sql, param):
        param = tuple(param)
        cursor = self.mysql.cursor()
        cursor.execute(sql, param)
        data = cursor.fetchone()
        return data

    def executeInsterSQL(self, tableName, key_arr, pla_arr, val_arr):
        val_arr = tuple(val_arr)
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (tableName, key_arr, pla_arr)
        try:
            cursor = self.mysql.cursor()
            cursor.execute(sql, val_arr)
            pass
        except:
            if not self.hasMySqlTableForDB(tableName):
                self.createMySqlTable(tableName)
            tabKetArr = self.getMySqlFieldNameByTable(tableName)
            key_list = key_arr.split(',')
            for i in range(0, len(key_list)):
                key = key_list[i]
                naked = key.replace('`', '')
                if naked == 'key' or naked == 'id' or naked == '_id':
                    unique = True
                else:
                    unique = False
                if (naked,) not in tabKetArr:
                    if isinstance(val_arr[i], int):
                        self.createMySqlFieldToTable(tableName, naked, 'INT(11)', unique=unique)
                    elif isinstance(val_arr[i], float) or isinstance(val_arr[i], long):
                        self.createMySqlFieldToTable(tableName, naked, 'DOUBLE', unique=unique)
                    elif naked in self.text_column:  # 检查特殊字段(TEXT)
                        self.createMySqlFieldToTable(tableName, naked, 'TEXT', unique=unique)
                    else:
                        self.createMySqlFieldToTable(tableName, naked, 'VARCHAR(256)', unique=unique)
            cursor = self.mysql.cursor()
            try:
                cursor.execute(sql, val_arr)
            except Exception, e:
                if e[0] == 1062:
                    return
                cursor.execute(sql, val_arr)
        self.mysql.commit()

    def executeInsterSQLOfMultiterm(self, tableName, key_arr, pla_arr, val_arr_list):
        val_arr = val_arr_list[0]
        for i in range(0, len(val_arr_list)):
            val_arr_list[i] = tuple(val_arr_list[i])
        val_arrs = tuple(val_arr_list)
        sql = 'INSERT INTO %s (%s) VALUES(%s)' % (tableName, key_arr, pla_arr)
        try:
            cursor = self.mysql.cursor()
            cursor.executemany(sql, val_arrs)
        except:
            if not self.hasMySqlTableForDB(tableName):
                self.createMySqlTable(tableName)
            tabKetArr = self.getMySqlFieldNameByTable(tableName)
            key_list = key_arr.split(',')
            for i in range(0, len(key_list)):
                key = key_list[i]
                naked = key.replace('`', '')
                if naked in unique_column:
                    unique = True
                else:
                    unique = False
                if naked not in tabKetArr:
                    if isinstance(val_arr[i], int):
                        self.createMySqlFieldToTable(tableName, naked, 'INT(11)', unique=unique)
                    elif isinstance(val_arr[i], float) or isinstance(val_arr[i], long):
                        self.createMySqlFieldToTable(tableName, naked, 'DOUBLE', unique=unique)
                    elif 'dra' in naked or 'summary' in naked:
                        self.createMySqlFieldToTable(tableName, naked, 'TEXT', unique=unique)
                    else:
                        self.createMySqlFieldToTable(tableName, naked, 'VARCHAR(256)', unique=unique)
            cursor = self.mysql.cursor()
            cursor.executemany(sql, val_arrs)
        self.mysql.commit()

    def getMySqlFieldNameByTable(self, tableName):
        base_sql = "select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' and table_schema = '%s'"
        cursor = self.mysql.cursor()
        cursor.execute(base_sql % (tableName, self.mysql_db))
        data = cursor.fetchall()
        return data

    def getMySqlTableName(self):
        base_sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s'"
        cursor = self.mysql.cursor()
        cursor.execute(base_sql % (self.mysql_db))
        data = cursor.fetchall()
        return data

    def hasMySqlTableForDB(self, tableName):
        base_sql = "SELECT COUNT(TABLE_NAME) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME='%s'"
        cursor = self.mysql.cursor()
        cursor.execute(base_sql % (self.mysql_db, tableName))
        data = cursor.fetchone()
        return data[0] > 0
