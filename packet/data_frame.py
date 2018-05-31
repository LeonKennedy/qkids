#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: data_frame.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 好像只能说是qkids基础数据DataFrame
# @Create: 2018-05-30 14:30:57
# @Last Modified: 2018-05-30 14:30:57


import sys, pdb, os
sys.path.append('..')
import pandas as pd
import numpy as np
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection
from produce_month_index import MonthIndexFactroy, MonthIndex


small_product_id = (4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 29, 
1003, 1004, 1005, 1006, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
1033, 1034, 1035, 1036, 1037, 1038)
big_product_id = (5, 6, 13, 19)
all_product_id = big_product_id + small_product_id

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

class StatisticsDataHouse:

  def __init__(self):
    pass

class FisrtBuyMonthStudent:
  
  # FisrtBuyMonth * Student
  def __init__(self, category=1):
    self.filename = 'data/firstbuymonth_student.pkl'
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

    self.conn = get_bills_connection()
    self.cash_bill_conn = get_cash_billing_connection()
    self.out_dataframe = None

  def scan_records(self, date):
    months = MonthIndexFactroy(date)
    buy_set = set()
    dataframe = pd.DataFrame(0, index = months.index, columns=self.student_list)
    for m in months.output:
      print('fetch month %s from database' % m.name)
      for row in self.get_student_by_month(m):
        sid = row[0]
        if sid in buy_set:
          continue
        buy_set.add(sid)
        if sid in dataframe.columns:
          #dataframe[row[0]] = 0
          dataframe.at[m.name, sid] = row[1]
    self.out_dataframe = dataframe

  def get_student_by_month(self, month):
    with self.conn.cursor() as cur:
      sql = "select student_id, sum(actual_price) from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 80) and student_id > 0 and \
      deleted_at is null group by student_id" % (month.begin, month.end, str(self.product_ids))
      cur.execute(sql)
      data = cur.fetchall()
    if month.name < '2018-03':
      return data
    with self.cash_bill_conn.cursor() as cur:
      sql = "select student_id, sum(actual_price) from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 80) and student_id > 0 and \
      deleted_at is null group by student_id" % (month.begin, month.end, str(self.cash_product_ids))
      cur.execute(sql)
    return data + cur.fetchall()

  def get_dataframe(self, date=None):
    if os.path.isfile(self.filename):
      return pd.read_pickle(self.filename)
    else:
      self.student_list = get_student_list()
      self.scan_records(date)
      print('save data frame to %s' % self.filename)
      pd.to_pickle(self.out_dataframe, self.filename)
      return self.out_dataframe


class ConsumeMonthStudent:

  def __init__(self, category=1):
    self.filename = 'data/consumemonth_student.pkl'
    self.bill_conn = get_bills_connection()
    self.legacy_conn = get_product_connection()

  def scan_records(self, date=None):
    months = MonthIndexFactroy(date)
    dataframe = pd.DataFrame(0, index = months.index, columns=self.student_list)
    for m in months.output:
      print('fetch month %s from database' % m.name)
      for row in self.get_records_by_month(m):
        sid = row[0]
        if sid in dataframe.columns:
          dataframe.at[m.name, sid] += float(row[1])
    self.out_dataframe = dataframe
    
  def get_records_by_month(self, m):
    if m.name < '2017-08':
      with self.legacy_conn.cursor() as cur:
        sql = "select user_id, sum(lesson_count) from consumes where created_at  \
        between %r and %r and status_id = 2 and deleted_at is null group by \
        user_id" % (m.begin, m.end)
        cur.execute(sql)
        return cur.fetchall()
    elif m.name == '2017-08':
      legacy_data = list()
      new_data = None
      with self.legacy_conn.cursor() as cur:
        sql = "select user_id, sum(lesson_count) from consumes where created_at  \
        between %r and %r and status_id = 2 and deleted_at is null group by \
        user_id" % (m.begin, m.end)
        cur.execute(sql)
        legacy_data = list(cur.fetchall())
      with self.bill_conn.cursor() as cur:
        sql  = "select student_id, sum(lesson_count) from student_consumptions use \
        index(created_at_idx) where created_at between %r and %r and \
        status = 2 and deleted_at is null group by student_id" % (m.begin, m.end)
        cur.execute(sql)
        new_data = cur.fetchall()
      legacy_data.extend(new_data)
      return legacy_data
    else:
      with self.bill_conn.cursor() as cur:
        sql  = "select student_id, sum(lesson_count) from student_consumptions use \
        index(created_at_idx) where created_at between %r and %r and \
        status = 2 and deleted_at is null group by student_id" % (m.begin, m.end)
        cur.execute(sql)
        return cur.fetchall()

  def get_dataframe(self, date=None):
    if os.path.isfile(self.filename):
      return pd.read_pickle(self.filename)
    else:
      self.student_list = get_student_list()
      self.scan_records(date)
      print('save data frame to %s' % self.filename)
      pd.to_pickle(self.out_dataframe, self.filename)
      return self.out_dataframe

if __name__ == "__main__":
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe('2018-05')
  c = ConsumeMonthStudent()
  cb = c.get_dataframe('2018-01')
  pdb.set_trace()
  print(cb)

