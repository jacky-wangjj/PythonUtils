#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------
# Author:   wangjj17
# Name:     MysqlUtils
# Date:     2019/4/3
# -------------------------
import pymysql

create_table_sql = """create table student (
                      id char(20),
                      name char(20) not  null,
                      adress char(20),
                      tel char(11),
                      sex char(1),
                      age int,
                      score float )"""
insert_sql = """insert into student (id, name, adress, tel, sex, age, score) 
                values ('%s', '%s', '%s', '%s', '%s', %s, %s)"""

def createTable():
    # 打开数据库连接
    db = pymysql.connect(host='10.110.181.39', user='priestuser', passwd='priestuser', db='test', port=3306, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    cursor.execute(create_table_sql)
    # 提交到数据库执行
    db.commit()
    # 关闭数据库
    db.close()

def insertData(line):
    # 打开数据库连接
    db = pymysql.connect(host='10.110.181.39', user='priestuser', passwd='priestuser', db='test', port=3306, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    for i in range(0, line):
        id = str(i)
        sql = insert_sql % (id, 'LiMing', 'beijing', '18011112345', 'M', 18, 100)
        # 使用execute方法执行SQL语句
        cursor.execute(sql)
        if i%100 == 0:
            # 提交到数据库执行
            db.commit()
    # 关闭数据库
    db.close()

def getTableSize(database, table):
    sql = """select concat(round(sum(data_length / 1024 / 1024), 2), 'MB') as data_length_MB,
             concat(round(sum(index_length / 1024 / 1024), 2), 'MB') as index_length_MB
             from tables where table_schema = ? and table_name = ?"""
    # 打开数据库连接
    db = pymysql.connect(host='10.110.181.39', user='priestuser', passwd='priestuser', db='test', port=3306,
                         charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # 使用execute方法执行SQL语句
    cursor.execute(sql % (database, table))
    # 提交到数据库执行
    db.commit()
    # 关闭数据库
    db.close()

if __name__ == "__main__":
    createTable()
    insertData(1000000000)
    getTableSize('test', 'student')