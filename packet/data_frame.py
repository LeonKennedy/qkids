#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: data_frame.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 好像只能说是qkids基础数据DataFrame
# @Create: 2018-05-30 14:30:57
# @Last Modified: 2018-05-30 14:30:57


import sys, pdb, os, logging
sys.path.append('..')
import pandas as pd
import numpy as np
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection
from produce_month_index import MonthIndexFactroy, MonthIndex
from data_frame_base import BaseQkidsDataFrame


class StatisticsDataHouse:

  def __init__(self):
    pass

class FisrtBuyMonthStudent(BaseQkidsDataFrame):
  
  # FisrtBuyMonth * Student
  def __init__(self, category=1, statistics_type = 'sum'):
    super(FisrtBuyMonthStudent, self).__init__(category, statistics_type)
    self.filename = 'data/firstbuymonth_student.pkl'

    self.conn = get_bills_connection()
    self.cash_bill_conn = get_cash_billing_connection()

  def scan_records(self, months):
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

  def get_student_by_tag(self, tag=1):
    self.log.info('get student by tag %d' % tag)
    df = self.out_dataframe
    split_price = 0
    select_student_columns = list()
    filename = None
    # 大单/长期用户
    if tag == 1:
      filename = 'data/vip_student_series.pkl'
      split_price = 1000
    else:
      pass

    if os.path.isfile(filename):
      return pd.read_pickle(filename)
    else:
      for c in df.columns:
        if (df.loc[:,c]  > split_price).any():
          select_student_columns.append(c)
      vip_columns = pd.Series(select_student_columns)
      pd.to_pickle(vip_columns, filename)
      return vip_columns

class ConsumeMonthStudent(BaseQkidsDataFrame):

  def __init__(self, category=1, statistics_type = 'sum'):
    super(ConsumeMonthStudent, self).__init__(category, statistics_type)
    self.filename = 'data/consumemonth_student.pkl'

    self.bill_conn = get_bills_connection()
    self.legacy_conn = get_product_connection()
    self.cash_bill_conn = get_cash_billing_connection()

  def scan_records(self, months=None):
    dataframe = pd.DataFrame(0, index = months.index, columns=self.student_list)
    for m in months.output:
      self.log.info('fetch month %s from database' % m.name)
      for row in self.get_records_by_month(m):
        sid = row[0]
        if sid in dataframe.columns:
          dataframe.at[m.name, sid] += 1 if self.statistics_type == 'count' else float(row[1])
    self.out_dataframe = dataframe
    
  def get_records_by_month(self, m):
    if m.name < '2017-08':
      with self.legacy_conn.cursor() as cur:
        sql = "select user_id, lesson_count ,date_format(created_at, '%%Y-%%m-%%d') from consumes where created_at  \
        between %r and %r and status_id = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        return cur.fetchall()
    elif m.name == '2017-08':
      legacy_data = list()
      new_data = None
      with self.legacy_conn.cursor() as cur:
        sql = "select user_id, lesson_count , date_format(created_at, '%%Y-%%m-%%d') from consumes where created_at  \
        between %r and %r and status_id = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        legacy_data = list(cur.fetchall())
      with self.bill_conn.cursor() as cur:
        sql  = "select student_id, lesson_count , date_format(created_at, '%%Y-%%m-%%d') from student_consumptions use \
        index(created_at_idx) where created_at between %r and %r and \
        status = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        new_data = cur.fetchall()
      legacy_data.extend(new_data)
      return legacy_data
    else:
      with self.bill_conn.cursor() as cur:
        sql  = "select student_id, lesson_count ,date_format(created_at, '%%Y-%%m-%%d') from student_consumptions use \
        index(created_at_idx) where created_at between %r and %r and \
        status = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        data = cur.fetchall()
      if m.name < '2018-03':
        return data
      with self.cash_bill_conn.cursor() as cur:
        sql = "select student_id, case room_type when 2 then 1.5 when 5 then 2.5 \
        else 3 end , date_format(created_at, '%%Y-%%m-%%d') from student_consumptions where created_at between %r and %r and \
        status = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        return data + cur.fetchall() 


class LessonStudent(BaseQkidsDataFrame):
  def __init__(self, category = 1):
    super(LessonStudent, self).__init__(category, 'count')
    self.filename = 'data/lesson_student.pkl'

    self.conn = get_course_connection()
    self.schedule_conn = get_schedule_connection()
    self.product_conn = get_product_connection()
    self.get_course_lesson()

  def scan_records(self,month):
    lessons = self.get_course_lesson()
    vip_student_list = self.get_student_by_tag()
    dataframe = pd.DataFrame(0, index = lessons, columns=vip_student_list)
    for m in month.output:
      self.log.info('fetch month %s from databse' % m.name)
      for row in self.get_records_by_month(m):
        sid = row[0]
        lid = row[1]
        if lid in dataframe.index and sid in dataframe.columns:
          dataframe.loc[row[1], row[0]] += 1
      pd.to_pickle(dataframe, self.filename)
      self.log.info('saved to file: %s' % self.filename)
    self.out_dataframe = dataframe

  def get_records_by_month(self, m):
    legacy_data = None
    data = None
    #if m.name <= '2018-03':
    if False:
      with self.product_conn.cursor() as cur:
        sql = "select user_id, lesson_id from user_schedules where locked_at \
        between %r and %r and status_id = 40 and deleted_at is null" % (m.begin, m.end)
        cur.execute(sql) 
        legacy_data = cur.fetchall()
    if m.name >= '2017-05':
      with self.schedule_conn.cursor() as cur:
        sql = "select student_id, lesson_id, begin_at from student_appointments sa \
        join schedules s on sa.schedule_id = s.id and begin_at between %r and %r and \
        s.is_internal = 0 and s.deleted_at is null where status = 3 and sa.deleted_at \
        is null" % (m.begin, m.end)
        cur.execute(sql)
        return cur.fetchall()

  def get_course_lesson(self):
    self.log.info('get course chapter lesson stduent from database')
    self.course_lesson = dict()
    lessons = list()
    with self.conn.cursor() as cur:
      sql = "select course_id, l.id from lessons l left join chapters c on c.id = l.chapter_id \
      where course_id between 1 and 12"
      cur.execute(sql)
      for row in cur.fetchall():
        lid = int(row[1])
        lessons.append(lid)
        if row[0] in self.course_lesson.keys():
          self.course_lesson[row[0]].append(lid)
        else:
          self.course_lesson[row[0]] = [lid]
    return lessons

  def aggregate_by_course(self):
    self.log.info('get course student matrix')
    df = pd.DataFrame()
    for c,ls in self.course_lesson.items():
      df[c] = self.out_dataframe.loc[ls,].sum()
    self.course_dataframe = df
    return df

if __name__ == "__main__":
  #f = FisrtBuyMonthStudent()
  #fb = f.get_dataframe()
  #vip_columns = f.get_student_by_tag(1)
  #func = lambda x: 0 if x == 0 else 1
  #f = fb.filter(items = vip_columns)
  #f = f.applymap(func)
  pass
  #cb = c.get_dataframe()
  #c = cb.filter(items= vip_columns)
  #a = f.dot(c.T) 
