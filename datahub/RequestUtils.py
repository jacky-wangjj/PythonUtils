#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------
# Author:   wangjj17
# Name:     RequestUtils
# Date:     2019/4/4
# -------------------------
import json
import time

import jsonpath
import requests

# requests.Session对象可以在多个http请求之间保持变量、公用cookie、保持长连接从而提高性能等。
session = requests.Session()

headers = {
    'Host': 'node16.sleap.com:8089',
    'Referer': 'http://node16.sleap.com:8089/leapid-admin/view/login.html?cb=http%3A%2F%2Fnode15.sleap.com%3A2017',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
}

login_url = 'http://node16.sleap.com:8089/leapid-admin/api/v1/login'

def get_config():
    file = open('config.json')
    config = json.load(file)
    # print(config)
    projectList = jsonpath.jsonpath(config, '$..project')
    dbTypeList = jsonpath.jsonpath(config, '$..dbType')
    hostList = jsonpath.jsonpath(config, '$..host')
    dbNameList = jsonpath.jsonpath(config, '$..dbName')
    portList = jsonpath.jsonpath(config, '$..port')
    userNameList = jsonpath.jsonpath(config, '$..userName')
    passWdList = jsonpath.jsonpath(config, '$..passWd')
    target_positionList = jsonpath.jsonpath(config, '$..target_position')
    conn_nameList = jsonpath.jsonpath(config, '$..conn_name')
    source_tableList = jsonpath.jsonpath(config, '$..source_table')
    target_dbList = jsonpath.jsonpath(config, '$..target_db')
    return projectList,dbTypeList,hostList,dbNameList,portList,userNameList,passWdList,target_positionList,conn_nameList,source_tableList,target_dbList

def login(login_url, username, passwd):
    params = {
        'un': username,
        'pw': passwd
    }
    response = session.post(login_url, data=params, headers= headers)
    # requests会自动管理cookies,通过requests get或post网页之后，若是第一次访问，在response headers里会有set-cookies字段，
    print('response headers:',response.headers)
    # requests会识别这些字段，同时在接下来的get\post中，自动添加这些cookies。
    # 登录成功之后服务器返回给客户端一个cookie sid，保存在session.cookies中。
    print('session cookies:',session.cookies)
    # print('result:',response.json()['result'])
    # print('data:',response.json()['data'])
    response.close()

create_user_url = 'http://node16.sleap.com:8089/leapid-admin/p/api/v1/leapid/'
def create_user(username, password, roleType, realname, email, phone, state):
    if roleType == 'admin':
        role = 'leapid.admin'
    elif roleType == 'pm':
        role = 'leapid.pm,sql,proc,dhub'
    elif roleType == 'member':
        role = 'leapid.member,sql,proc,dhub'
    params = {
        'username': username,
        'password': password,
        'roles': role,
        'realname': realname,
        'email': email,
        'phone': phone,
        'state': state,
        'department': '',
        'address': '',
        'remark': ''
    }
    response = session.post(create_user_url, data=params, headers=headers)
    print('response headers:', response.headers)
    print('session cookies:', session.cookies)
    print('result:',response.json()['result'])
    print('data:',response.json()['data'])
    response.close()

projects_url = 'http://node15.sleap.com:2017/leapid/projects'
def get_projects():
    now = int(time.time()*1000)
    params = {
        '_' : now
    }
    response = session.get(projects_url, params=params, headers=headers)
    # print('result:',response.json())
    model = response.json()['model']
    # print('model:',model)
    # print(model.keys())
    response.close()
    return model

config_url = 'http://node15.sleap.com:2017/hdfs/system/config'
def get_configs():
    now = int(time.time() * 1000)
    params = {
        '_': now
    }
    response = session.get(config_url, params=params, headers=headers)
    # print('result:',response.json())
    model = response.json()['model']
    # print('model:', model)
    # print(model.keys())
    response.close()
    return model

dbList_url = 'http://node15.sleap.com:2017/db/list'
def get_dbList(pids,dbType):
    now = int(time.time() * 1000)
    params = {
        'pids': pids,
        'pageSize': 1000,
        'pageNo': 1,
        'groups': 1,
        'dbType': dbType,
        '_': now
    }
    response = session.get(dbList_url, params=params, headers=headers)
    # print('result:',response.json())
    model = response.json()['model']
    data = model['data']
    # print('data:',data)
    # print(data[0])
    response.close()
    return data

dbDbs_url = 'http://node15.sleap.com:2017/db/dbs'
def get_dbs(id,pid):
    now = int(time.time() * 1000)
    params = {
        'id': id,
        'pid': pid,
        '_': now
    }
    response = session.get(dbDbs_url, params=params, headers=headers)
    model = response.json()['model']
    # print('model:',model)
    response.close()
    return model

tbls_url = 'http://node15.sleap.com:2017/db/table/tbls'
def get_tbls(dbId, catalog):
    now = int(time.time() * 1000)
    params = {
        'dbId': dbId,
        'catalog': catalog,
        '_': now,
    }
    response = session.get(tbls_url, params=params, headers=headers)
    # print('result:',response.json())
    model = response.json()['model']
    # print(model[0])
    response.close()
    return model

ddlBatch_url = 'http://node15.sleap.com:2017/db/table/ddl/batch'
def post_ddlBatch(fromId,toId,pid,fromTablesJson,toTablesJson):
    params = {
        'fromId': fromId,
        'toId': toId,
        'pid': pid,
        'fromTablesJson': fromTablesJson,
        'toTablesJson': toTablesJson
    }
    # print('batch params:',params)
    response = session.post(ddlBatch_url, data=params, headers=headers)
    # print('result:',response.json()
    model = response.json()['model']
    # print('model:', model)
    # print(response.status_code)
    # print(response.headers)
    # print(response.encoding)
    print(response.url)
    # print(model.keys())
    response.close()
    return model

createBatch_url = 'http://node15.sleap.com:2017/db/table/create/batch'
def post_createBatch(dbId,pid,tablesJsonStr):
    params = {
        'dbId': dbId,
        'pid': pid,
        'tablesJsonStr': tablesJsonStr
    }
    print('create batch params:', params)
    response = session.post(createBatch_url, data=params, headers=headers)
    return response

dbSave_url = 'http://node15.sleap.com:2017/job/db/save'
def post_save(pid,jobName,note,fromJson,periodType,fromId,toId,cronExpression,startTime,endTime,groupNo,toType,toHiveJson):
    params = {
        'pid': pid,
        'jobName': jobName,
        'note': note,
        'fromJson': fromJson,
        'periodType': periodType,
        'fromId': fromId,
        'toId': toId,
        'cronExpression': cronExpression,
        'startTimeStr': startTime,
        'endTimeStr': endTime,
        'groupNo': groupNo,
        'toType': toType,
        'toHiveJson': toHiveJson
    }
    print('save params:', params)
    response = session.post(dbSave_url, data=params, headers=headers)
    # print('result:',response.json()
    return response

def simple():
    login(login_url, 'leapadmin', 'leapadmin')
    username = 'test5'
    password = '123456'
    roleType = 'member'
    realname = 'test'
    email = ''
    phone = ''
    state = 0
    create_user(username, password, roleType, realname, email, phone, state)

def main_function():
    # 登录
    login(login_url, 'leapadmin', 'leapadmin')
    # 获取配置文件中的信息
    projectList,dbTypeList,hostList,dbNameList,portList,userNameList,passWdList,target_positionList,conn_nameList,source_tableList,target_dbList = get_config()
    size = len(dbTypeList)
    for i in range(size):
        if dbTypeList[i] == 'mysql':
            dbType = 38
        elif dbTypeList[i] == 'oracle':
            dbType = 4
        elif dbTypeList[i] == 'sqlserver':
            dbType = 40
        elif dbTypeList[i] == 'db2':
            dbType = 44
        elif dbTypeList[i] == 'postgresql':
            dbType = 24
        else:
            print('dbType error!')
            break
        # 获取项目名称
        projectModel = get_projects()
        for key in projectModel.keys():
            if projectModel[key]['name'] == projectList[i]:
                pid = projectModel[key]['id']
        if pid is None:
            print('"project": "'+projectList[i]+'" config error!')
            continue
        print('pid:',pid)
        # 获取连接配置信息
        configsModel = get_configs()
        if target_positionList[i] == 'HIVE':
            hiveParam = configsModel['hive']
            # print('hiveParam:',hiveParam)
            if hiveParam['connName'] == conn_nameList[i] and hiveParam['dbName'] == target_dbList[i]:
                toId = hiveParam['id']
        if toId is None:
            print('"conn_name": "'+conn_nameList[i]+'" or "target_db": "'+target_dbList[i]+'" config error!')
            continue
        print('toId',toId)
        # 获取连接数据库信息
        dbListData = get_dbList(pids=pid,dbType=dbType)
        for j in range(len(dbListData)):
            if dbListData[j]['dbName'] == dbNameList[i]:
                dbId = dbListData[j]['id']
        if dbId is None:
            print('"dbName": "'+dbNameList[i]+'" config error!')
            continue
        print('dbId:',dbId)
        dbsModel = get_dbs(id=dbId,pid=pid)
        fromTablesJson = []
        toTablesJson = []
        fromJson = []
        if source_tableList[i] == 'ALL':
            # 全选
            print('ALL')
            databases = dbsModel['databases']
            index = 0
            for k in range(len(databases)):
                db = databases[k]['name']
                tblsModel = get_tbls(dbId=dbId, catalog=db)
                tbls = jsonpath.jsonpath(tblsModel, '$..name')
                for m in range(len(tbls)):
                    fromTable = {
                        'catalog': db,
                        'schema': '',
                        'tableName': tbls[m],
                        'index': index
                    }
                    fromTablesJson.append(fromTable)
                    toTable = {
                        'catalog': '',
                        'schema': target_dbList[i],
                        'tableName': tbls[m]
                    }
                    toTablesJson.append(toTable)
                    fromJ = {
                        'id': dbId,
                        'name': db,
                        'catalog': db,
                        'schema': '',
                        'tableName': tbls[m],
                        'whereSql': ''
                    }
                    fromJson.append(fromJ)
                    index = index+1
        elif isinstance(source_tableList[i], list):
            # 选择指定数据库中的指定表
            index = 0
            for source in source_tableList[i]:
                # print(source)
                db = source["db"]
                tables = source["table"]
                print('db:', db)
                if tables == 'ALL':
                    tblsModel = get_tbls(dbId=dbId, catalog=db)
                    tbls = jsonpath.jsonpath(tblsModel, '$..name')
                    for m in range(len(tbls)):
                        fromTable = {
                            'catalog': db,
                            'schema': '',
                            'tableName': tbls[m],
                            'index': index
                        }
                        fromTablesJson.append(fromTable)
                        toTable = {
                            'catalog': '',
                            'schema': target_dbList[i],
                            'tableName': tbls[m]
                        }
                        toTablesJson.append(toTable)
                        fromJ = {
                            'id': dbId,
                            'name': db,
                            'catalog': db,
                            'schema': '',
                            'tableName': tbls[m],
                            'whereSql': ''
                        }
                        fromJson.append(fromJ)
                        index = index+1
                elif isinstance(tables, list):
                    # print('tables:', tables)
                    for table in tables:
                        fromTable = {
                            'catalog': db,
                            'schema': '',
                            'tableName': table,
                            'index': index
                        }
                        fromTablesJson.append(fromTable)
                        toTable = {
                            'catalog': '',
                            'schema': target_dbList[i],
                            'tableName': table
                        }
                        toTablesJson.append(toTable)
                        fromJ = {
                            'id': dbId,
                            'name': db,
                            'catalog': db,
                            'schema': '',
                            'tableName': table,
                            'whereSql': ''
                        }
                        fromJson.append(fromJ)
                        index = index+1
        print('fromTablesJson:',fromTablesJson)
        print('toTablesJson:',toTablesJson)
        batchModel = post_ddlBatch(fromId=dbId,toId=toId,pid=pid,fromTablesJson=json.dumps(fromTablesJson),toTablesJson=json.dumps(toTablesJson))
        if target_positionList[i] == 'HIVE':
            toType = 9
        # create/batch
        tablesJsonStr = []
        toHiveJson = []
        index = 0
        for h in range(len(batchModel)):
            model = batchModel[h]
            # print(model['msg'])
            print('return batch model',model['model'])
            if model['msg'] == 'show':
                toHive = {
                    'createSql': '',
                    'ddl': model['model']['ddl'],
                    'name': model['model']['schema'],
                    'tableName': model['model']['name'],
                    'catalog': '',
                    'schema': model['model']['schema'],
                    'id': toId,
                    'replaceEnter': 'true',
                    'replacement': ' ',
                    'tableType': 0,
                    'index': index,
                    'dbId': toId
                }
            elif model['msg'] == 'new':
                toHive = {
                    'createSql': '',
                    'ddl': '',
                    'name': model['model']['schema'],
                    'tableName': model['model']['name'],
                    'catalog': '',
                    'schema': model['model']['schema'],
                    'id': toId,
                    'replaceEnter': 'true',
                    'replacement': ' ',
                    'tableType': 0,
                    'index': index,
                    'dbId': toId
                }
            toHiveJson.append(toHive)
            tableJson = {
                'createSql': model['model']['ddl'],
                'ddl': model['model']['ddl'],
                'name': model['model']['schema'],
                'tableName': model['model']['name'],
                'catalog': '',
                'schema': model['model']['schema'],
                'id': toId,
                'replaceEnter': 'true',
                'replacement': ' ',
                'tableType': 0,
                'index': index,
                'dbId': toId
            }
            if model['msg'] == 'show':
                pass
            elif model['msg'] == 'new':
                tablesJsonStr.append(tableJson)
            index = index+1
        create_res = post_createBatch(dbId=toId, pid=pid, tablesJsonStr=json.dumps(tablesJsonStr))
        print('create batch:',create_res.url)
        print('create batch:',create_res.status_code)
        create_res.close()
        jobName='task'+str(i)
        save_res = post_save(pid=pid,jobName=jobName,note='',fromJson=json.dumps(fromJson),periodType=0,fromId=dbId,toId=toId,
                             cronExpression='* * * * * ?',startTime='',endTime='',groupNo=2,toType=toType,toHiveJson=json.dumps(toHiveJson))
        print('save:',save_res.url)
        print('save:',save_res.status_code)
        save_res.close()

if __name__ == "__main__":
    # simple()
    main_function()