#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: data_frame_base.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 基础类
# @Create: 2018-05-31 16:05:05
# @Last Modified: 2018-05-31 16:05:05
#

import logging, sys, os
import pandas as pd
from produce_month_index import MonthIndexFactroy, MonthIndex

logger = logging.getLogger("main")
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
file_handler = logging.FileHandler("produce.log")
file_handler.setFormatter(formatter)  
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter  

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.setLevel(logging.INFO)

class BaseQkidsDataFrame:

  def __init__(self, category=1, statistics_type = 'sum'):
    self.log =  logger
    self.log.info('initial %s', self.__class__)
    
    # 订单id
    small_product_id = (4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 29, 
      1003, 1004, 1005, 1006, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
      1033, 1034, 1035, 1036, 1037, 1038)
    big_product_id = (5, 6, 13, 19)
    all_product_id = big_product_id + small_product_id

    self.filename = 'data/temp.pkl'
    # 0 大小单 
    # 1 大单
    # 2 小单
    if category == 1:
      self.product_ids = big_product_id
      self.cash_product_ids = (2,3,4)
    elif category == 2:
      self.product_ids = small_product_id
      self.cash_product_ids = (1,)
    else:
      self.product_ids = all_product_id
      self.cash_product_ids = (1,2,3,4)

    # 结果模型
    self.out_dataframe = None

    # 数据聚合是sum 还是 count
    self.statistics_type = statistics_type

  def get_dataframe(self, m=None, refresh=False):
    if m is None:
      m = MonthIndexFactroy()

    filename = self.filename
    if self.statistics_type == 'count':
      filename = '_count.'.join(self.filename.split('.'))
    elif self.statistics_type == 'distinct':
      filename = '_distinct.'.join(self.filename.split('.')) 
    else:
      pass
      
    if not refresh and os.path.isfile(filename):
      self.log.info('get data frame from file')
      self.out_dataframe = pd.read_pickle(filename)
      return self.out_dataframe
    else:
      self.log.info('get data frame from database ')
      self.student_list = get_student_list()
      self.scan_records(m)
      self.log.info('save data frame to %s' % filename)
      pd.to_pickle(self.out_dataframe, filename)
      return self.out_dataframe

def get_student_list():
  filename = 'data/student_list'
  if os.path.isfile(filename):
    with open(filename, "r") as fo:
      data = fo.readlines()
    return [ int(i.replace("\n", "")) for i in data]
  conn = get_product_connection()
  with conn.cursor() as cur:
    sql = "select user_id from users where encrypt_mobile_v2 is not null and deleted_at is null"
    cur.execute(sql)
    data = [ str(i[0]) for i in cur.fetchall()]
    print('write to file: %s' % filename)
    with open(filename, 'w') as fo:
      fo.writelines([ i + '\n' for i in data])
  return data

if __name__ == "__main__":
  b = BaseQkidsDataFrame()
    
