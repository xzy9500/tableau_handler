# -*- coding: utf-8 -*-

import requests
import json
import pandas as pd

class TableauHandler():

    def __init__(self, server, user, pwd):
        self.main_url = 'http://%s/api/2.6' % server
        self.user = user
        self.pwd = pwd
        self.session = requests.session()
        self.header = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        # 进行登录，获得token、userid、siteid
        self.auth_data = self.signin()
        # 对siteid赋值
        self.utils_url = self.main_url + '/sites/{site_id}'.format(site_id=self.auth_data['credentials']['site']['id'])
        pass

    # 获取token、userid、siteid
    def signin(self):
        post_data = json.dumps({"credentials": {"name": self.user, "password": self.pwd, "site": {"contentUrl": ""}}})
        resp = self.session.post(self.main_url+'/auth/signin', data=post_data, headers=self.header)
        resp.raise_for_status()
        resp_data = json.loads(resp.content)
        # 更新header的token值
        self.header['X-Tableau-Auth'] = resp_data['credentials']['token']
        return resp_data

    # 获取用户信息
    def get_user(self):
        resp = self.session.get(self.utils_url+'/users', headers=self.header)
        return resp

    # 获取用户组信息
    def get_group(self):
        resp = self.session.get(self.utils_url + '/groups', headers=self.header)
        return resp

    # 获取项目信息
    def get_project(self):
        resp = self.session.get(self.utils_url+'/projects', headers=self.header)
        return resp

    # 获取工作薄信息
    def get_workbook(self):
        resp = self.session.get(self.utils_url+'/workbooks', headers=self.header)
        return resp

    # 获取数据源信息
    def get_datasource(self):
        resp = self.session.get(self.utils_url+'/datasources', headers=self.header)
        return resp

    # 获取任务
    def get_job(self):
        resp = self.session.get(self.utils_url+'/jobs', headers=self.header)
        return resp

    # 获取任务计划
    def get_schedule(self):
        resp = self.session.get(self.main_url+'/schedules', headers=self.header)
        return resp

    # 登出
    def signout(self):
        resp = self.session.post(self.main_url+'/auth/signout', data=b'', headers=self.header)
        resp.raise_for_status()
        return print('Sign out successful!')

    # 将tableau-server返回的中文乱码进行解码
    def decode_chn(self, s):
        return s.encode('raw_unicode_escape').decode()

    # 将响应对象中的数据装换成数据框
    def resp_to_dataframe(self, resp):
        resp.raise_for_status()
        df = pd.DataFrame(json.loads(resp.content))
        return df

if __name__ == '__main__':
    t_handle = TableauHandler('server', 'user', 'password')
    resp = t_handle.get_workbook()
    data = pd.DataFrame(json.loads(resp.content)['workbooks']['workbook'])
    data.to_csv('workbook.txt', index=False)
    t_handle.signout()
    t_handle.session.close()