#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :progress_test.py
# @Time      :2022/9/7 08:11
# @Author    :Colin
# @Note      :None
from concurrent.futures import *
import urllib.request

URLS = ['http://www.foxnews.com/',
        'http://www.cnn.com/',
        'http://europe.wsj.com/',
        'http://www.bbc.co.uk/',
        'http://some-made-up-domain.com/',
        'load_2',
        'load_1']


# Retrieve a single page and report the URL and contents
def load_1(url):
    # with urllib.request.urlopen(url, timeout=timeout) as conn:
    #     cont = conn.read()
    # print(url[:10])
    # if url[:10] == 'http://som':
    #     raise Exception\
    # print('load1', func)
    # print(url)
    if url == 'load_1':
        raise Exception
    return url


def load_2(url):
    if url == 'load_2':
        raise Exception
    return url

    # def __call__():
    #     return None


def load_3(url):
    if url == 'http://some-made-up-domain.com/':
        raise Exception
    # return url


# We can use a with statement to ensure threads are cleaned up promptly
with ThreadPoolExecutor(max_workers=2) as executor:
    # Start the load operations and mark each future with its URL
    def muti_task(url):
        load_3(load_2(load_1(url)))


    future_to_url = {executor.submit(muti_task, url): url for url in URLS}
    # future_to_url_2 = {executor.submit(load_2, url): url for url in future_to_url}
    # future_to_url_2 = {executor.submit(load_1, url): url for url in URLS}
    # print(future_to_url)
    for future in as_completed(future_to_url):
        url = future_to_url[future]  # 执行的表名
        # res = future.result()
        try:
            res = future.result()  # 线程执行的结果 ,没有返回就是None
            # print(future)
        except Exception as exc:
            # 异常执行
            print('%r generated an exception: %s' % (url, exc))
        else:
            # 正常执行
            print('%r ok' % url)
