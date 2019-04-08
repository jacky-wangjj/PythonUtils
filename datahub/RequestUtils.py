#!/usr/bin/python
# -*- coding: utf-8 -*-
# -------------------------
# Author:   wangjj17
# Name:     RequestUtils
# Date:     2019/4/4
# -------------------------

import requests

# requests.Session对象可以在多个http请求之间保持变量、公用cookie、保持长连接从而提高性能等。
session = requests.Session()

headers = {
    'Host': 'node16.sleap.com:8089',
    'Referer': 'http://node16.sleap.com:8089/leapid-admin/view/login.html?cb=http%3A%2F%2Fnode15.sleap.com%3A2017',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'
}

login_url = 'http://node16.sleap.com:8089/leapid-admin/api/v1/login'

def login(login_url, username, passwd):
    params = {
        'un': username,
        'pw': passwd
    }
    response = session.post(login_url, params=params, headers= headers)
    # requests会自动管理cookies,通过requests get或post网页之后，若是第一次访问，在response headers里会有set-cookies字段，
    print('response headers:',response.headers)
    # requests会识别这些字段，同时在接下来的get\post中，自动添加这些cookies。
    # 登录成功之后服务器返回给客户端一个cookie sid，保存在session.cookies中。
    print('session cookies:',session.cookies)
    print('result:',response.json()['result'])
    print('data:',response.json()['data'])

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
    response = session.post(create_user_url, params=params, headers=headers)
    print('response headers:', response.headers)
    print('session cookies:', session.cookies)
    print('result:',response.json()['result'])
    print('data:',response.json()['data'])

if __name__ == "__main__":
    login(login_url, 'leapadmin', 'leapadmin')
    username = 'test5'
    password = '123456'
    roleType = 'member'
    realname = 'test'
    email = ''
    phone = ''
    state = 0
    create_user(username,password,roleType,realname,email,phone,state)