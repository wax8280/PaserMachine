# !/usr/bin/env python
# coding: utf-8
from pyquery import PyQuery as pq
from multiprocessing import Pool
import json
import sqlite3
import os
from setting import *
from functools import partial


class Paser:
    name = 'allitebook'
    raw_data_path = 'result'
    path = [os.path.join(i[0], j) for i in os.walk(os.path.join(os.path.join(RAW_DATE_ROOT_PATH, name), raw_data_path))
            for j in i[2]]

    @staticmethod
    def _connect_db():
        return sqlite3.connect(os.path.join(PERSISTENCE_DB_ROOT_PATH, Paser.name + '.db'))

    @staticmethod
    def build_db():
        conn = Paser._connect_db()
        conn.execute(
            """CREATE TABLE RESULT
            (
            MAIN_TITLE VARCHAR,
            SUB_TITLE VARCHAR,
            ISBN VARCHAR,
            AUTHOR VARCHAR,
            CATEGORY VARCHAR,
            FILE_FORMAT VARCHAR,
            FILE_SIZE VARCHAR,
            LANGUAGE VARCHAR,
            PAGES VARCHAR,
            YEAR VARCHAR,
            DESCRIPTION VARCHAR,
            DOWNLOAD_URL VARCHAR,
            IMG_URL VARCHAR
            )
            """
        )

def parse(file_path):
    conn = Paser._connect_db()
    Q = pq(filename=file_path)

    item = dict()
    d = []
    item.setdefault('download_url', Q('.download-links a').attr.href)
    item.setdefault('main_title', Q('h1.single-title').text())
    item.setdefault('sub_title', Q('.entry-header h4').text())
    item.setdefault('img_url', Q('div.entry-body-thumbnail a img').attr.src)
    names = [i.text().replace(':', '').replace(' ', '_').lower() for i in Q('.book-detail dt').items()]
    values = [i.text() for i in Q('.book-detail dd').items()]
    for k, v in zip(names, values):
        item.setdefault(k, v)
    item['description'] = '\n'.join(
        [i.text().replace('Book Description: ', '') for i in Q('.entry-content').items()])

    for i in item.keys():
        if i not in ["main_title", "sub_title", "isbn", "author", "category", "file_format", "file_size",
                     "language", "pages", "year", "description", "download_url", "img_url"]:
            print (i)

    for k in ["main_title", "sub_title", "isbn", "author", "category", "file_format", "file_size",
              "language", "pages", "year", "description", "download_url", "img_url"]:
        try:
            d.append(item[k])
        except KeyError:
            d.append('')

    conn.executemany('INSERT INTO RESULT VALUES (?,?,?,?,?,?,?,?,?,?,?,?)', [d])
    conn.commit()


def run():
    pool = Pool()
    path = Paser.path
    pool.map(parse, path)
