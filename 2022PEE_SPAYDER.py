import numpy as np
import requests
from bs4 import BeautifulSoup
import re
from lxml import etree
import random
import pymongo
import time
from multiprocessing import Pool


# 建立无用户名密码连接
myclient = pymongo.MongoClient('mongodb://localhost:27017/')
# 设置数据库名称
mydb = myclient['2022考研']
# 设置表名
information = mydb['调剂2022-03-14']

# 设置请求头
headers={
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36'}

def get_info(url):
    res = requests.get(url,headers=headers,verify=False)
    # 利用BeautifulSoup库，精准定位需要爬取的内容，无需正则表达式
    soup = BeautifulSoup(res.text, 'lxml')
    # 获取帖子的title信息，包含帖子名称和链接
    titles = soup.select('.forum_body_manage .xmc_lp20')
    # 获取其他信息
    colleges = soup.select('.forum_body_manage .xmc_lp20+ td')
    majors = soup.select('.forum_body_manage td:nth-child(3)')
    nums = soup.select('.forum_body_manage td:nth-child(4)')
    times = soup.select('.forum_body_manage td:nth-child(5)')

    for title,college,major,num,ti in zip(titles,colleges,majors,nums,times):
        try:
            u=title.a['href']
        except KeyError:
            u="null"
        info = {
            '链接': u,
            '标题': title.get_text(),
            '院校': college.get_text(),
            '专业': major.get_text(),
            '调剂人数': num.get_text(),
            '时间':ti.get_text()
            }
        information.insert_one(info)

    print(url + 'Done')
    # 设置爬取间隔
    time.sleep(random.randint(3, 8))

if __name__=='__main__':
    # 爬取小木虫调剂信息
    urls = ['http://muchong.com/bbs/kaoyan.php?&page={}'.format(str(i))
            # 设置爬取页数
            for i in range(1,40)]
    # 设置线程数
    pool = Pool(processes=4)
    pool.map(get_info, urls)