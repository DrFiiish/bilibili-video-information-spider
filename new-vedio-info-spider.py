# -*- coding: utf-8 -*-
"""
Created on Tue Jul 31 16:58:51 2018

@author: peter
"""

import time
import sqlite3
import requests
import asyncio
total = 1
result = []
flag = None
conn = None

head = {
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
                  '(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
}


async def request(url):
    future = loop.run_in_executor(
        None, requests.get, url, head)
    response = await future
    response.encoding = response.apparent_encoding
    demo = response.text.replace('null','None')
    return demo


async def run(url):
    # 启动爬虫
    global total
    req = await request(url)
    req = eval(req)
    try:
        data = req['data']
        video = (
            total,
            data['aid'],        # 视频编号
            data['view'],       # 播放量
            data['danmaku'],    # 弹幕数
            data['reply'],      # 评论数
            data['favorite'],   # 收藏数
            data['coin'],       # 硬币数
            data['share']       # 分享数
        )
        result.append(video)
        if total % 250 == 0:
            global time0
            time1 = time.time()
            print("爬取{0}个网页 ，总花费时间:{1:.2f}s".format(
                total, time1-time0))
        total += 1
    except:
        pass


def create():
    # 创建数据库
    global conn
    conn = sqlite3.connect('video-data.db')
    conn.execute("""create table if not exists data
                    (id int prinmary key autocrement,
                    aid int,
                    view int,
                    danmaku int,
                    reply int,
                    favorite int,
                    coin int,
                    share int)""")


def save():
    # 将数据保存至本地
    global result, conn, flag, total
    command = "insert into data \
             values(?,?,?,?,?,?,?,?);"
    for row in result:
        try:
            conn.execute(command, row)
        except:
            conn.rollback()
            flag = "error has occurred when total is "+str(total)
            pass
    conn.commit()
    result = []


if __name__ == "__main__":
    create()
    time0 = time.time()
    for i in range(0, 2700):
        begin = 10000*i
        urls = ["http://api.bilibili.com/archive_stat/stat?aid={}".format(j)
                for j in range(begin, begin+10000)]
        tasks = [asyncio.ensure_future(run(url)) for url in urls]
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.wait(tasks))
        save()
    if flag != None:
        print(flag)
        flag = None
    conn.close()
