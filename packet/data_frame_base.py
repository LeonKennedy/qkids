#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: data_frame_base.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 基础类
# @Create: 2018-05-31 16:05:05
# @Last Modified: 2018-05-31 16:05:05
#

import logging, sys, os, pdb
sys.path.append('..')
import pandas as pd
from produce_month_index import MonthIndexFactroy, MonthIndex
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection
from collections import namedtuple

logger = logging.getLogger("main")
formatter = logging.Formatter('%(asctime)s %(levelname)-8s: %(message)s')
file_handler = logging.FileHandler("produce.log")
file_handler.setFormatter(formatter)  
console_handler = logging.StreamHandler(sys.stdout)
console_handler.formatter = formatter  

logger.addHandler(file_handler)
logger.addHandler(console_handler)

logger.setLevel(logging.INFO)

ProductIds = namedtuple('ProductIds','name bill_ids cash_bill_ids')
ExperienceProduct = ProductIds('experience', (), ())

def get_product_ids_from_database(t):
  bill_conn = get_bills_connection()
  sql = "select group_concat(id) from products where billing_type = %d" % t
  with bill_conn.cursor() as cur:
    cur.execute(sql)
    data = cur.fetchone()
  return tuple( int(i) for i in data[0].split(','))
  
def get_experience_product():
  return ProductIds('experience', get_product_ids_from_database(1), (1, 999))

def get_format_product():
  return ProductIds('format', get_product_ids_from_database(2), (2,3,4))

class BaseDataFrame:
  experience_product_id  = (4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 27,28, 29,
      1003, 1004, 1005, 1006, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
      1033, 1034, 1035, 1036, 1037, 1038)
  format_product_id = (5, 6, 13, 19)

  def __init__(self):
    self.log = logger
    self.log.info('initial %s', self.__class__)
    self.all_product_id  = self.experience_product_id +  self.format_product_id
    #self.student_list = get_student_series()

  def set_vip_student_list(self):
    self.vip_student_list = get_vip_student_series(True)

  def get_dataframe(self, refresh=False):
    filename = self.filename
    if not refresh and os.path.isfile(filename):
      print('get data from file: %s' % filename)
      return pd.read_pickle(filename)
    else:
      print('get data from database')
      self.transfer_to_data_frame()
      print('save to file: %s' % filename)
      pd.to_pickle(self.out_dataframe, filename)
      return self.out_dataframe


class BaseQkidsDataFrame:

  def __init__(self, category=3, statistics_type = 'sum'):
    self.log =  logger
    self.log.info('initial %s', self.__class__)
    
    # 订单id
    small_product_id = (4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 27,28, 29, 
      1003, 1004, 1005, 1006, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
      1033, 1034, 1035, 1036, 1037, 1038, 1039, 1040, 1041, 1043, 1044)
    big_product_id = (5, 6, 13, 19, 1057, 1058)
    all_product_id = big_product_id + small_product_id

    self.filename = 'data/temp.pkl'
    # 3 大小单 
    # 1 大单
    # 2 小单
    self.category = category
    if category == 1:
      self.product_ids = big_product_id
      self.cash_product_ids = (2,3,4)
    elif category == 2:
      self.product_ids = small_product_id
      self.cash_product_ids = (1, 999)
    else:
      self.product_ids = all_product_id
      self.cash_product_ids = (1,2,3,4)

    # 结果模型
    self.out_dataframe = None

    # 数据聚合是sum 还是 count
    self.statistics_type = statistics_type
    # student
    self.student_list = get_student_list()

  def increase_dataframe(self):
    m = MonthIndexFactroy()
    tm = m.get_this_month()
    for s in self.student_list:
      if s > self.out_dataframe.columns[-1]:
        self.out_dataframe.insert(self.out_dataframe.columns.size,s, 0)
    if tm.name not in self.out_dataframe.index:
      last_m = m.get_last_month()
      self.out_dataframe.loc[tm.name] = 0
      self.increase_singleton(last_m)
    self.increase_singleton(tm)
    pd.to_pickle(self.out_dataframe, self.filename)
    
  def get_dataframe(self, m=None, refresh=False):
    if m is None:
      m = MonthIndexFactroy()

    filename = self.filename
    filename = ('_'+str(self.category)+'.').join(self.filename.split('.')) 
    if self.statistics_type == 'count':
      filename = '_count.'.join(filename.split('.'))
    elif self.statistics_type == 'distinct':
      filename = '_distinct.'.join(filename.split('.')) 
    else:
      pass
    self.filename = filename
      
    if not refresh and os.path.isfile(filename):
      self.log.info('get data frame from file')
      self.out_dataframe = pd.read_pickle(filename)
      self.increase_dataframe()
      return self.out_dataframe
    else:
      self.log.info('get data frame from database ')
      self.scan_records(m)
      self.log.info('save data frame to %s' % filename)
      pd.to_pickle(self.out_dataframe, filename)
      return self.out_dataframe

  def get_student_by_tag(self, tag = 1):
    filename = 'data/vip_student_series.pkl'
    if os.path.isfile(filename):
      return pd.read_pickle(filename)

def get_student_series(refresh=False):
  filename = 'data/students.pkl'
  if not refresh and os.path.isfile(filename):
    print('read student from filename: %s' % filename)
    return pd.read_pickle(filename)
  else:
    print('read students from database')
    conn = get_product_connection()
    with conn.cursor() as cur:
      sql = "select user_id from users where encrypt_mobile_v2 is not null and deleted_at is null"
      cur.execute(sql)
      data = [ i[0] for i in cur.fetchall()]
    s = pd.Series(data, dtype='uint32')
    print('save student to file: %s' % filename)
    pd.to_pickle(s, filename)
    return s

def get_student_list(refresh=False):
  filename = 'data/student_list'
  if os.path.isfile(filename):
    with open(filename, "r") as fo:
      data = fo.readlines()
    student_list = [ int(i.replace("\n", "")) for i in data]
    inc_students = get_increate_students(student_list[-1])
    if len(inc_students) > 0:
      print('write to file: %s' % filename)
      with open(filename, 'w') as fo:
        fo.writelines([ str(i) + '\n' for i in student_list + inc_students])
    return student_list + inc_students
  conn = get_product_connection()
  with conn.cursor() as cur:
    sql = "select user_id from users where encrypt_mobile_v2 is not null and deleted_at is null"
    cur.execute(sql)
    data = [ i[0] for i in cur.fetchall()]
    print('write to file: %s' % filename)
    with open(filename, 'w') as fo:
      fo.writelines([ str(i) + '\n' for i in data])
  return data

def get_vip_student_series(refresh=True):
  filename = 'data/vip_students.pkl'
  if os.path.isfile(filename):
    print('get vip students from file: %s' % filename)
    return pd.read_pickle(filename)
  conn = get_product_connection()
  with conn.cursor() as cur:
    sql = "select user_id from users where first_large_buy_at is \
    not null and encrypt_mobile_v2 is not null and deleted_at is \
    null"
    cur.execute(sql)
    data = [ i[0] for i in cur.fetchall()]
    s = pd.Series(data, dtype='uint32')
    print('save vip student to file: %s' % filename)
    pd.to_pickle(s, filename)
    return s
  
def get_increate_students(sid):
  return []
  conn = get_product_connection()
  print('check the increase student')
  with conn.cursor() as cur:
    sql = "select user_id from users where user_id > %d and encrypt_mobile_v2 is not null and deleted_at is null" % int(sid)
    cur.execute(sql)
    return [ i[0] for i in cur.fetchall()]
  
if __name__ == "__main__":
  b = BaseQkidsDataFrame()
  c = get_experience_product()
  pdb.set_trace()
  print(c.name)
    
