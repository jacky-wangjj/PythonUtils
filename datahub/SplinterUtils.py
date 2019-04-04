#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------
# Author:   wangjj17
# Name:     SplinterUtils
# Date:     2019/4/4
# -------------------------
from time import sleep
from splinter.browser import Browser
import json
import jsonpath

driver_name = 'chrome'
executable_path = 'D:\ML\Code\chromedriver.exe'
driver = Browser(driver_name=driver_name, executable_path=executable_path)
driver.driver.set_window_size(1400, 1000)

login_url='http://node16.sleap.com:8089/leapid-admin/view/login.html?cb=http%3A%2F%2Fnode15.sleap.com%3A2017'
datahub_url='http://node15.sleap.com:2017/'

mysql_href='./db?type=1&dbtype=38'
oracle_href='./db?type=1&dbtype=4'
sqlserver_href='./db?type=1&dbtype=40'
db2_href='./db?type=1&dbtype=44'
postgresql_href='./db?type=1&dbtype=24'

def get_config():
    file = open('config.json')
    config = json.load(file)
    # print(config)
    dbTypeList = jsonpath.jsonpath(config, '$..dbType')
    hostList = jsonpath.jsonpath(config, '$..host')
    dbNameList = jsonpath.jsonpath(config, '$..dbName')
    portList = jsonpath.jsonpath(config, '$..port')
    userNameList = jsonpath.jsonpath(config, '$..userName')
    passWdList = jsonpath.jsonpath(config, '$..passWd')
    target_positionList = jsonpath.jsonpath(config, '$..target_position')
    conn_nameList = jsonpath.jsonpath(config, '$..conn_name')
    target_dbList = jsonpath.jsonpath(config, '$..target_db')
    return dbTypeList,hostList,dbNameList,portList,userNameList,passWdList,target_positionList,conn_nameList,target_dbList

def login(login_url, username, passwd):
    driver.visit(login_url)
    driver.find_by_id('username').fill(username)
    driver.find_by_id('password').fill(passwd)
    driver.find_by_id('FormLoginBtn').click()

def add_db_resources(url, dbHost, dbName, username, passwd):
    driver.visit(url)
    driver.find_by_text(u"资源库信息管理").click()
    driver.find_by_text(u"数据库连接信息").click()
    driver.find_by_text(u" + 新建资源库信息").click()
    # 服务器地址
    driver.find_by_xpath('//*[@data-key="host"]').fill(dbHost)
    # 数据库dbName
    driver.find_by_xpath('//*[@data-key="dbName"]').fill(dbName)
    # 用户名
    driver.find_by_xpath('//*[@data-key="userName"]').fill(username)
    # 密码
    driver.find_by_xpath('//*[@data-key="password"]').fill(passwd)
    # driver.find_by_text(u"测试连接").click()
    driver.find_by_text(u"确定").click()
    # driver.find_by_text(u"Hive连接信息").click()
    # driver.find_by_text(u"Hdfs连接信息").click()

def import_data_from_mysql(url, href, taskName, db_res_name, target_position, conn_name, target_db):
    driver.visit(url)
    driver.find_link_by_href(href).click()
    # 查找字段为class="taskName"的element
    driver.find_by_xpath('//*[@class="taskName"]').fill(taskName)
    driver.find_option_by_text(db_res_name).click()
    driver.find_option_by_text(target_position).click()
    driver.find_option_by_text(conn_name).click()
    driver.find_by_text(u"下一步").click()
    # 选择导入表，全选
    sleep(1)
    driver.find_by_xpath('//*[@class="allSelected check"]').click()
    driver.find_by_xpath('//*[@class="move-right"]').click()
    driver.find_by_xpath('//*[@class="check allSelected"]').click()
    driver.find_by_xpath('//*[@class="batch"]').click()
    driver.find_by_id('select2-batch-dbName-container').click()
    # 查找id中包含default字段的element
    driver.find_by_xpath('//*[contains(@id, "'+target_db+'")]').click()
    driver.find_by_text(u"保存").last.click()
    sleep(5)
    driver.find_by_text(u"下一步").last.click()
    driver.find_by_text(u"开始导入").click()

def simple():
    # 登录
    login(login_url, 'leapadmin', 'leapadmin')
    # 添加资源连接
    host = '10.110.181.39'
    dbName = 'leapid'
    port = '3306'
    userName = 'leapid'
    passWd = 'leapid-db-pwd'
    add_db_resources(datahub_url, host, dbName, userName, passWd)
    # 导入数据
    task_name = 't2'
    # 10.110.181.39_3306_leapid
    db_res_name = host + "_" + port + "_" + dbName
    target_position = 'HIVE'
    conn_name = 'LEAP_SYSTEM_HIVE'
    target_db = 'default'
    import_data_from_mysql(datahub_url, mysql_href, task_name, db_res_name, target_position, conn_name, target_db)

def main_function():
    # 登录
    login(login_url, 'leapadmin', 'leapadmin')
    # 获取配置文件中的信息
    dbTypeList, hostList, dbNameList, portList, userNameList, passWdList, target_positionList, conn_nameList, target_dbList = get_config()
    size = len(dbTypeList)
    for i in range(size):
        # 添加资源连接
        print("添加第"+str(i)+"个资源连接")
        print(datahub_url,hostList[i],dbNameList[i],userNameList[i],passWdList[i])
        add_db_resources(datahub_url,hostList[i],dbNameList[i],userNameList[i],passWdList[i])

    for i in range(size):
        # 导入数据
        task_name = 'task'+str(i)
        db_res_name = hostList[i]+"_"+portList[i]+"_"+dbNameList[i]
        if dbTypeList[i] == 'mysql':
            href = mysql_href
        elif dbTypeList[i] == 'oracle':
            href = oracle_href
        elif dbTypeList[i] == 'sqlserver':
            href = sqlserver_href
        elif dbTypeList[i] == 'db2':
            href = db2_href
        elif dbTypeList[i] == 'postgresql':
            href = postgresql_href
        else:
            break
        print("导入第"+str(i)+"个资源连接中所有数据")
        print(datahub_url,href,task_name,db_res_name,target_positionList[i],conn_nameList[i],target_dbList[i])
        import_data_from_mysql(datahub_url,href,task_name,db_res_name,target_positionList[i],conn_nameList[i],target_dbList[i])

if __name__ == "__main__":
    main_function()
