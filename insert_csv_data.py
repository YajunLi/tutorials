# -*-coding : utf-8 -*-
import pandas as pd
import os
import re
import sqlalchemy
import psycopg2
import time
import io
from unrar import rarfile
from threading import Thread
import gc
import logging
from functools import reduce
from sqlalchemy import create_engine

import numpy as np
from threading import Lock
logger = logging.getLogger('TickDataToDatabase')
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("log_file\\log_%s.txt" %pd.datetime.now().date())
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
handler.setLevel(logging.INFO)
logger.addHandler(handler)

# os.system(r'net use \\192.168.31.10\stock_ticks')

dir_2019 = r'E:\数据文件'
files = reduce(lambda x, y: x+y, list(map(lambda x: list(map(lambda z: os.path.join(x[0], z), x[2])), \
                                          os.walk(dir_2019))))
counter_lock = Lock()
columns_name = ['c_market', 'c_stock_code', 'c_date_time', 'c_price', 'c_current',
                'c_money', 'c_vol', 'c_flag', 'c_buy1_price', 'c_buy2_price',
                'c_buy3_price', 'c_buy4_price', 'c_buy5_price', 'c_sell1_price',
                'c_sell2_price', 'c_sell3_price', 'c_sell4_price', 'c_sell5_price',
                'c_buy1_quantity', 'c_buy2_quantity', 'c_buy3_quantity',
                'c_buy4_quantity', 'c_buy5_quantity', 'c_sell1_quantity',
                'c_sell2_quantity', 'c_sell3_quantity', 'c_sell4_quantity',
                'c_sell5_quantity']

conn = create_engine('postgres+psycopg2://julin:123456@localhost:5432/BrooksCapital', echo=True)

# 目标代码
target_file_codes = pd.read_excel('stock_pool.xlsx', header=None, encoding='ANSI')
target_file_codes.dropna(axis=1, inplace=True)
stock_code_pattern = re.compile(r'\d{6}')
target_file_codes = list(map(lambda x: stock_code_pattern.findall(x)[0], target_file_codes[1].tolist()))

date_pattern = re.compile(r'\d{4}')
long_date_pattern = re.compile(r'\d{6}')
# TODO 筛选文件
rar_file = list(filter(lambda x: date_pattern.findall(x)[0] and  x.endswith('.rar'), files))
file_status = False  # 文件状态


def get_csv_from_rar():
    """将rar文件解压到本地tmp文件目录下"""
    # TODO 筛选文件
    file_list = list(filter(lambda x: date_pattern.findall(x)[0] and x.endswith('.rar'), files))

    global stock_code_pattern, logger, file_status
    while 1:
        try:
            file = file_list.pop()
            try:
                rar = rarfile.RarFile(file, pwd=r'www.licai668.cn')
            except:
                logger.info("{} rar failed".format(file))
                continue
            date = long_date_pattern.findall(file)[0] if len(long_date_pattern.findall(file)) else \
            date_pattern.findall(file)[0]
            rar_csv = list(filter(lambda x: stock_code_pattern.findall(x)[0] in target_file_codes, rar.namelist()))
            tmp_dir = r'tmp\\%s' %date
            if not os.path.exists(tmp_dir):
                os.mkdir(tmp_dir)
        except IndexError:
            print('file over')
            logger.info('rar file read over!!!')
            break
        for csv in rar_csv:
            rar.extract(csv,tmp_dir)
    file_status = True


def insert_csv_to_database():
    """
    将csv数据读取到数据库中
    :param file_queue:  csv数据列表
    :return: no return
    """
    global  columns_name, logger, file_status
    conn = create_engine('postgres+psycopg2://postgres:123456@localhost:5432/postgres', echo=True)
    while 1:
        try:
            files = list(reduce(lambda x, y: x+y, list(map(lambda x: list(map(lambda z: os.path.join(x[0], z), x[2])),\
                                          os.walk(r'tmp')))))

            file = files.pop()
            logger.info('get csv file :%s' % file)

        except IndexError:
            print("queue empty, please hold on")
            if file_status:
                break
            else:
                time.sleep(10)
                logger.warn("trying to get rar files")
                continue
        except Exception as info:
            logger.info(info)
            continue

        # 读取csv文件，并导入至数据库
        try:
            data = pd.read_csv(file, encoding='ANSI', dtype=str)
            data.columns = columns_name
            data["c_date"] = data["c_date_time"].map(lambda x: x[:10])  # 取出日期部分信息
            data.drop_duplicates(subset=['c_stock_code', 'c_date_time'], keep='last', inplace=True)
        except Exception as info:
            logger.info(info)
            continue

        pd_sql_engine = pd.io.sql.pandasSQL_builder(conn)
        string_data_io = io.StringIO()
        data.to_csv(string_data_io, sep='|', index=False)
        try:
            # pd.io.sql.to_sql(data,'stocktick',con=conn,schema='public',if_exists='append', index=False)
            table = pd.io.sql.SQLTable('stock_tick', pd_sql_engine, frame=data,
                                       index=False, if_exists='append', schema='public')
            table.create()
            string_data_io.seek(0)
            string_data_io.readline()  # remove header
            with conn.connect() as connection:
                with connection.connection.cursor() as cursor:
                    copy_cmd = "COPY public.stock_tick FROM STDIN HEADER DELIMITER '|' CSV"
                    cursor.copy_expert(copy_cmd, string_data_io)
                connection.connection.commit()

        except psycopg2.IntegrityError as info:
            os.remove(file)
            logger.info(info)
            continue

        del data

        while 1:
            try:
                os.remove(file)
                break
            except Exception as e:
                logger.info('%s :data del failed \n %s' % (file, e))
                continue
        logger.info('%s :data write over ' % file)
        gc.collect()


def main():

    p2 = Thread(target=get_csv_from_rar,)
    p2.start()
    p3 = Thread(target=insert_csv_to_database, )
    p3.start()

    p3.join()
    logger.info('finish working!!')


if __name__ == '__main__':
    main()
