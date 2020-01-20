# -*- coding: utf-8 -*-
"""
Created on Sat Mar  3 17:48:23 2018

@author: peter
"""
import time
import sqlite3
import requests
import logging
import sys

logging.basicConfig(level=logging.INFO,
                    handlers=[logging.FileHandler(filename='bilibili-spider.log', mode='a', encoding='utf-8'),
                              logging.StreamHandler(sys.stdout)],
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
headers = {
    'X-Requested-With': 'XMLHttpRequest',
    'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36'
                   '(KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36')
}
total = 1
conn = None


def run(url):
    # 启动爬虫
    global total
    result = []
    req = requests.get(url, headers=headers, timeout=6).json()
    time.sleep(0.1)  # 延迟，避免太快 ip 被封
    try:
        if req['code'] != 0:
            #           print("视频不存在")
            return
        data = req['data']
        video = (
            total,
            data['aid'],  # 视频编号
            data['view'],  # 播放量
            data['danmaku'],  # 弹幕数
            data['reply'],  # 评论数
            data['favorite'],  # 收藏数
            data['coin'],  # 硬币数
            data['share']  # 分享数
        )
        result.append(video)
        if total % 10 == 0:
            global time0
            time1 = time.time()
            logging.info("爬取{0}个非404网页 ，总花费时间:{1:.2f}s".format(
                total, time1 - time0))
        total += 1
        save(result)
    except Exception as e:
        logging.error(e)


def create():
    # 创建数据库
    global conn
    conn = sqlite3.connect('data.db')
    conn.execute("""create table if not exists data
                    (id int prinmary key autocrement,
                    aid int,
                    view int,
                    danmaku int,
                    reply int,
                    favorite int,
                    coin int,
                    share int)""")
    conn.execute("""insert into data
                    (id,aid,view,danmaku,reply,favorite,coin,share)    
                    values(0,0,0,0,0,0,0,0)""")


def save(result=[]):
    # 将数据保存至本地
    global conn, total
    command = "insert into data \
             values(?,?,?,?,?,?,?,?);"
    for row in result:
        try:
            conn.execute(command, row)
        except Exception as e:
            conn.rollback()
            logging.error("error has occurred when result is " + str(row))
            logging.error("error is " + str(e))
    conn.commit()


if __name__ == "__main__":
    create()
    time0 = time.time()
    for i in range(1, 1981 * 10000):
        begin = 10000 * i
        url = "https://api.bilibili.com/archive_stat/stat?aid=" + str(i)
        run(url)
    conn.close()
