# !/usr/bin/env python
# coding: utf-8
from pyquery import PyQuery as pq
from multiprocessing import Pool
import sqlite3
import os
from setting import *
from collections import OrderedDict
from lib.commom import splite_list

# 分割200份,份得越多,数据库操作越频繁,内存占用越少
DIVIDE = 200

name = 'verycd_china'
raw_data_path = 'china_result'
path = [os.path.join(i[0], j) for i in
        os.walk(os.path.join(os.path.join(RAW_DATE_ROOT_PATH, name), raw_data_path)) for j in i[2]]
splite_path_list = splite_list(path, DIVIDE)


def _connect_db():
    return sqlite3.connect(os.path.join(PERSISTENCE_DB_ROOT_PATH, name + '.db'))


def build_db():
    conn = _connect_db()
    # ED2K_URL_AND_SIZE 为JSON字符串
    conn.execute(
        """CREATE TABLE RESULT
        (
        TITLE VARCHAR,
        NAME VARCHAR,
        CHINESE_NAME VARCHAR,
        ACTOR VARCHAR,
        PUBLISH_TIME VARCHAR,
        COUNTRY VARCHAR,
        LANGUAGE VARCHAR,
        ITEM_PUBLISH_TIME VARCHAR,
        ITEM_UPDATE_TIME VARCHAR,
        FILE_TYPE VARCHAR,
        CONTENT VARCHAR,
        TRACK VARCHAR,
        ED2K_URL_AND_SIZE VARCHAR,
        FLORDER_IMG_URL VARCHAR
        )
        """
    )


def parse(file_path):
    conn = _connect_db()

    result = []
    for filename in file_path:
        Q = pq(filename=filename)
        item = OrderedDict()
        d = []
        item.setdefault('TITLE', Q('#topicstitle').text())
        if Q('#iptcomEname'):
            item.setdefault('NAME', Q('#iptcomEname').text().split(':')[1])
        else:
            item.setdefault('NAME', '')
        item.setdefault('CHINESE_NAME', Q('#iptcomCname').text().replace('专辑中文名', '').replace(':', '').strip())
        item.setdefault('ACTOR', Q('#iptcomActor .iptcom-info a').text())
        if Q('#iptcomTime'):
            item.setdefault('PUBLISH_TIME', Q('#iptcomTime').text().replace(u'发行时间', u'').
                            replace(u':', u'').strip().split(' ')[0])
        else:
            item.setdefault('PUBLISH_TIME', '')
        if Q('#iptcomCountry a'):
            item.setdefault('COUNTRY', Q('#iptcomCountry a').text().split(' ')[0])
        else:
            item.setdefault('COUNTRY', '')
        if Q('#iptcomLanguage a'):
            item.setdefault('LANGUAGE', Q('#iptcomLanguage a').text().split(' ')[0])
        else:
            item.setdefault('LANGUAGE', '')
        item.setdefault('ITEM_PUBLISH_TIME', Q('.block11 li span.date-time').eq(0).text())
        item.setdefault('ITEM_UPDATE_TIME', Q('.block11 li span.date-time').eq(1).text())
        if Q('#iptcomFiletype'):
            item.setdefault('FILE_TYPE', Q('#iptcomFiletype').text().split(':')[1])
        else:
            item.setdefault('FILE_TYPE', '')
        item.setdefault('CONTENT', Q('#iptcomContents p.inner_content').html())
        item.setdefault('TRACK', Q('#iptcomTrack p.inner_content').html())

        item.setdefault('ED2K_URL_AND_SIZE', str([(tr('td[align]').text(), tr('a[href^="ed2k"]').attr.href)
                                                  for tr in Q('div.emulemain table tr').items() if
                                                  tr('a[href^="ed2k"]').attr.href]))

        item.setdefault('FLORDER_IMG_URL', Q('#topicImgUrl img').attr.src)

        for k in item.keys():
            try:
                d.append(item[k])
            except KeyError:
                d.append('')
        result.append(d)

    conn.executemany('INSERT INTO RESULT VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', result)
    conn.commit()


def run():
    pool = Pool()
    pool.map(parse, splite_path_list)
