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
from collections import Counter
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection
from produce_month_index import MonthIndexFactroy, MonthIndex
from data_frame_base import BaseQkidsDataFrame,BaseDataFrame


class StatisticsDataHouse:

  def __init__(self):
    pass


class BillDataFrame(BaseDataFrame):
  def __init__(self, category = 3):
    super(BillDataFrame, self).__init__()
    self.bill_conn = get_bills_connection()
    self.cash_bill_conn = get_cash_billing_connection()
    self.product_conn = get_product_connection()

    self.category = category
    if category == 1:
      self.product_ids = self.format_product_id
      self.cash_product_ids = (2,3,4)
    elif category == 2:
      self.product_ids = self.experience_product_id
      self.cash_product_ids = (1, 999)
    else:
      self.product_ids = self.all_product_id
      self.cash_product_ids = (1,2,3,4)
  
  def get_money_records_by_month(self, month):
    with self.bill_conn.cursor() as cur:
      self.log.info('fetch month %s from bill database' % month.name)
      sql = "select student_id, 0, product_id, actual_price from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 70, 80) and student_id > 0 and \
      deleted_at is null " % (month.begin, month.end, str(self.product_ids))
      cur.execute(sql)
      data = cur.fetchall()
    if month.name < '2018-03':
      return data
    with self.cash_bill_conn.cursor() as cur:
      self.log.info('fetch month %s from cash_bill database' % month.name)
      sql = "select student_id, 1, product_id, actual_price from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 70, 80) and pay_channel <> 'migration' \
      and student_id > 0 and deleted_at is null and product_type = 0 " % (month.begin, month.end, str(self.cash_product_ids))
      cur.execute(sql)
      return data + cur.fetchall()

  def get_lesson_records_by_month(self, month):
    with self.bill_conn.cursor() as cur:
      self.log.info('fetch month %s from bill database' % month.name)
      sql = "select student_id, lesson_count, product_id from bills b join products p \
      on b.product_id  = p.id where paid_at between \
      %r and %r and product_id in %s and status in (20, 70, 80) and student_id > 0 and \
      b.deleted_at is null " % (month.begin, month.end, str(self.product_ids))
      cur.execute(sql)
      data = cur.fetchall()
    if month.name < '2018-03':
      return data
    with self.cash_bill_conn.cursor() as cur:
      self.log.info('fetch month %s from cash_bill database' % month.name)
      sql = "select student_id, actual_price/35, product_id from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 70, 80) and pay_channel <> 'migration' \
      and student_id > 0 and deleted_at is null and product_type = 0 " % (month.begin, month.end, str(self.cash_product_ids))
      cur.execute(sql)
      return data + cur.fetchall()

  def get_lesson_gift_by_month(self, month):
    with self.bill_conn.cursor() as cur:
      self.log.info('fetch month %s from bill database' % month.name)
      sql = "select student_id, lesson_count, product_id from bills b join products p \
      on b.product_id  = p.id where paid_at between \
      %r and %r and product_id in %s and status in (20, 70, 80) and student_id > 0 and \
      b.deleted_at is null " % (month.begin, month.end, str(self.product_ids))
      # 只有大单才算 赠送
      if self.category == 1:
        sql = "select b.student_id, sp.lesson_count + ifnull(sp2.lesson_count,0) from bills b \
        join student_products sp on sp.bill_id  = b.id and sp.deleted_at is null \
        left join student_products sp2 on sp2.parent_id = sp.id and sp2.deleted_at is null \
        and sp2.lesson_count = 15 and sp2.admin_id = 0 \
        where paid_at between %r and %r and b.product_id in %s and b.status in (20, 70, 80) \
        and b.student_id > 0 and b.deleted_at is null " % (month.begin, month.end, str(self.product_ids))
      cur.execute(sql)
      data = cur.fetchall()
    if month.name < '2018-03':
      return data
    with self.cash_bill_conn.cursor() as cur:
      self.log.info('fetch month %s from cash_bill database' % month.name)
      sql = "select student_id, actual_price/35, product_id from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 70, 80) and pay_channel <> 'migration' \
      and student_id > 0 and deleted_at is null and product_type = 0 " % (month.begin, month.end, str(self.cash_product_ids))
      cur.execute(sql)
      return data + cur.fetchall()
    
  def transfer_to_data_frame(self, monthz = None):
    self.init_dataframe()
    if monthz is None:
      monthz = MonthIndexFactroy()

    for m in monthz.output:
      rows = self.get_records(m)
      counter = self.handle_records(rows)

      self.out_dataframe.loc[m.name] = 0
      self.fill_data(m,counter)
    self.after_dataframe()

  def fill_data(self, month, counter):
      for k,v in counter.items():
        if k in self.out_dataframe.columns:
          self.out_dataframe.at[month.name, k] = v

  def after_dataframe(self):
    pass
    

class FirstBuyMonthStudent(BillDataFrame):
  def __init__(self, category = 3):
    super(FirstBuyMonthStudent, self).__init__(category)
    self.filename = 'data/first_buy_student.pkl'
    self.buy_set = set()
    self.get_records = self.get_money_records_by_month
    self.handle_records = self.handle_records_first
    
  def init_dataframe(self):
    self.out_dataframe = pd.DataFrame(0, index = ('2015-12',), columns = (0,),dtype='uint8')
    
  def after_dataframe(self):
    self.out_dataframe.pop(0)

  def handle_records_first(self, rows):
    counter = Counter()
    for row in rows:
      sid = row[0]
      if sid not in self.buy_set:
        counter[sid] = 1
        self.buy_set.add(sid)
    return counter

  def fill_data(self, month, counter):
      for k,v in counter.items():
        self.out_dataframe.at[:,k] = 0
        self.out_dataframe.at[month.name, k] = v

  def create_vip_student_series(self):
    filename = 'data/vip_students.pkl'
    select_student_columns = list()
    df = self.out_dataframe
    for c in df.columns:
      if (df.loc[:,c]  > 0).any():
        select_student_columns.append(c)
    vip_columns = pd.Series(select_student_columns)
    print('save vip serires')
    pd.to_pickle(vip_columns, filename)
    return vip_columns

#  每月购买课时数
class BillingMonthStudent(BillDataFrame):
  def __init__(self, category =3):
    super(BillingMonthStudent, self).__init__(category)
    self.filename = 'data/bill_month_student.pkl'
    self.get_records = self.get_lesson_records_by_month
    self.handle_records = self.handle_records_sum

  def init_dataframe(self):
    self.out_dataframe = pd.DataFrame(0, index = ('2015-12',), columns = self.students ,dtype='float32')
  def handle_records_sum(self, rows):
    counter = Counter()
    for row in rows:
      sid = row[0]
      counter[sid] += float(round(row[1], 1))
    return counter

# ================= Refunds ==========

class RefundDataFrame(BillDataFrame):
  def __init__(self, category = 3):
    super(RefundDataFrame, self).__init__(category)
    self.filename = 'data/refund_month_student.pkl'
    self.get_records = self.get_refund_records_by_month
    self.handle_records = self.handle_records_sum

  def init_dataframe(self):
    self.out_dataframe = pd.DataFrame(0, index = ('2015-12',), columns = self.students ,dtype='float32')
    
  def get_refund_records_by_month(self, month):
    self.log.info('fetch month %s from bill database' % month.name)
    with self.bill_conn.cursor() as cur:
      sql = "select b.student_id, r.price, case r.type when 0 then  \
      sp.lesson_count - lesson_count_used else legacy_lessons end from \
      bills b join refunds r on b.id = r.bill_id and r.type in (0, 3)  and r.status = 3 and  \
      r.deleted_at is null join student_products sp on b.id = sp.bill_id \
      where paid_at between %r and %r and b.product_id  in %s and b.status in \
      (20, 70, 80) and b.deleted_at is null" % (month.begin, month.end, str(self.product_ids))
      cur.execute(sql)
      data = cur.fetchall()
      return data

  def handle_records_sum(self, rows):
    counter = Counter()
    for row in rows:
      f = row[2]
      counter[row[0]] += round(f,1)
    return counter


class FisrtBuyMonthStudent(BaseQkidsDataFrame):
  
  # FisrtBuyMonth * Student
  def __init__(self, category=1, statistics_type = 'sum'):
    super(FisrtBuyMonthStudent, self).__init__(category, statistics_type)
    self.filename = 'data/firstbuymonth_student.pkl'

    self.conn = get_bills_connection()
    self.cash_bill_conn = get_cash_billing_connection()

  def scan_records(self, months):
    dataframe = pd.DataFrame(0, index = months.index, columns=self.student_list, dtype='uint8')
    buy_set = set()
    for m in months.output:
      counter = Counter()
      self.log.info('fetch month %s from database' % m.name)
      for row in self.get_student_by_month(m):
        sid = row[0]
        if self.statistics_type == 'count':
          counter[sid] += 1
        elif self.statistics_type == 'distinct':
          if sid not in buy_set:
            counter[sid] = 1
            buy_set.add(sid)
        else:
          counter[sid] += round(row[1],1)

      for k,v in counter.items():
        if k in dataframe.columns:
          dataframe.at[m.name, k] = v
    self.out_dataframe = dataframe

  def increase_singleton(self, m):
    if not self.statistics_type == 'sum':
      return 
    counter = Counter()
    buy_set = set()
    for row in self.get_student_by_month(m):
      sid = row[0]
      if sid not in buy_set :
        counter[sid] += int(row[1])
        buy_set.add(sid)

    for k,v in counter.items():
      if k in self.out_dataframe.columns:
        self.out_dataframe.at[m.name, k] = v

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
      deleted_at is null and product_type = 0 group by student_id" % (month.begin, month.end, str(self.cash_product_ids))
      cur.execute(sql)
    return data + cur.fetchall()


  def get_records_by_month(self, month):
    with self.conn.cursor() as cur:
      sql = "select student_id, 0, product_id, actual_price from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 80) and student_id > 0 and \
      deleted_at is null " % (month.begin, month.end, str(self.product_ids))
      cur.execute(sql)
      data = cur.fetchall()
    if month.name < '2018-03':
      return data
    with self.cash_bill_conn.cursor() as cur:
      sql = "select student_id, 1, product_id, actual_price from bills where paid_at between \
      %r and %r and product_id in %s and status in (20, 80) and student_id > 0 and \
      deleted_at is null and product_type = 0 " % (month.begin, month.end, str(self.cash_product_ids))
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
      print('save vip serires')
      pd.to_pickle(vip_columns, filename)
      return vip_columns

  def create_vip_student_series(self):
    filename = 'data/vip_students.pkl'
    select_student_columns = list()
    split_price = 1000
    if self.statistics_type == 'distinct':
      split_price = 0
    df = self.out_dataframe
    for c in df.columns:
      if (df.loc[:,c]  > split_price).any():
        select_student_columns.append(c)
    vip_columns = pd.Series(select_student_columns)
    print('save vip serires')
    pd.to_pickle(vip_columns, filename)
    return vip_columns

class ConsumeMonthStudent(BaseQkidsDataFrame):

  def __init__(self, category=1, statistics_type = 'sum'):
    super(ConsumeMonthStudent, self).__init__(category, statistics_type)
    self.filename = 'data/consumemonth_student.pkl'

    self.bill_conn = get_bills_connection()
    self.legacy_conn = get_product_connection()
    self.cash_bill_conn = get_cash_billing_connection()


  def increase_singleton(self, m):
    counter = Counter()
    for row in self.get_records_by_month(m):
      sid = row[0]
      counter[sid] += 1 if self.statistics_type == 'count' else float(row[1])

    for k,v in counter.items():
      if k in self.out_dataframe.columns:
        self.out_dataframe.at[m.name, k] = v

  def scan_records(self, months=None):
    dataframe = pd.DataFrame(0, index = months.index, columns=self.student_list, dtype='uint16')
    buy_set = set()
    for m in months.output:
      self.log.info('fetch month %s from database' % m.name)
      for row in self.get_records_by_month(m):
        sid = row[0]
        if sid in dataframe.columns :
          if self.statistics_type == 'count':
            dataframe.at[m.name, sid] += 1
          elif self.statistics_type == 'distinct':
            if sid not in buy_set:
              dataframe.at[m.name, sid] = float(row[1])
              buy_set.add(sid)
          else:
            dataframe.at[m.name, sid] += float(row[1])
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
        sql = "select student_id, case room_type when 2 then 1 when 5 then 1.8 \
        else 3 end , date_format(created_at, '%%Y-%%m-%%d') from student_consumptions where created_at between %r and %r and \
        status = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        return data + cur.fetchall() 


class ScheduleDataFrame(BaseDataFrame):
  def __init__(self):
    super(ScheduleDataFrame, self).__init__()
    self.conn = get_schedule_connection()
    self.product_conn = get_product_connection()

  def get_student_apointment_records_by_month(self, m):
    self.log.info('fetch month %s from schedule database' % m.name)
    legacy_data = tuple()
    data = tuple()
    if m.name <= '2018-03':
      with self.product_conn.cursor() as cur:
        sql = "select user_id, room_type_id , s.lesson_id , begin_at from user_schedules us \
        join schedules s on  us.schedule_id = s.schedule_id and begin_at between %r and \
        %r and internal = 0 and s.deleted_at is null where status_id = 40 and us.deleted_at \
        is null" % (m.begin, m.end)
        cur.execute(sql) 
        legacy_data = cur.fetchall()
    if m.name >= '2017-05':
      with self.conn.cursor() as cur:
        sql = "select student_id, room_type_id, lesson_id, begin_at from student_appointments sa \
        join schedules s on sa.schedule_id = s.id and begin_at between %r and %r and \
        s.is_internal = 0 and s.deleted_at is null where status = 3 and sa.deleted_at \
        is null" % (m.begin, m.end)
        cur.execute(sql)
        data = cur.fetchall()
    return data + legacy_data

class LessonStudent(BaseQkidsDataFrame):
  def __init__(self, category = 1):
    super(LessonStudent, self).__init__(category, 'count')
    self.filename = 'data/lesson_student.pkl'

    self.conn = get_course_connection()
    self.schedule_conn = get_schedule_connection()
    self.product_conn = get_product_connection()
    self.get_course_lesson()

  def scan_records(self,month):
    if self.out_dataframe is None:
      lessons = self.get_course_lesson()
      vip_student_list = self.get_student_by_tag()
      self.out_dataframe = pd.DataFrame(0, index = lessons, columns=vip_student_list, dtype='uint8')
    dataframe = self.out_dataframe
    for m in month.output:
      self.log.info('fetch month %s from databse' % m.name)
      for row in self.get_records_by_month(m):
        sid = row[0]
        lid = row[1]
        if lid in dataframe.index and sid in dataframe.columns:
          dataframe.loc[row[1], row[0]] += 1
      pd.to_pickle(dataframe, self.filename)
      self.log.info('saved to file: %s' % self.filename)

  def get_records_by_month(self, m):
    legacy_data = None
    data = None
    if m.name <= '2018-03':
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

  def increase_dataframe(self):
    pass

  def increase_singleton(self, m):
    pass

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

 
class AppointmentStudent(ScheduleDataFrame):
  def __init__(self, category = 1):
    super(AppointmentStudent, self).__init__()
    self.filename = 'data/appointment_student.pkl'
    #self.get_records = self.get_refund_records_by_month
    #self.handle_records = self.handle_records_sum

  def transfer_to_data_frame(self,monthz = None):
    self.init_dataframe()
    if monthz is None:
      monthz = MonthIndexFactroy()
    for m in monthz.output:
      rows = self.get_student_apointment_records_by_month(m)
      counter = self.handle_records(rows)

      self.out_dataframe.loc[m.name] = 0
      self.fill_data(m, counter)

  def init_dataframe(self):
    self.out_dataframe = pd.DataFrame(0, index = ('2015-12',), columns = self.students ,dtype='uint16')
    
  def fill_data(self, month, counter):
      for k,v in counter.items():
        if k in self.out_dataframe.columns:
          self.out_dataframe.at[month.name, k] = v

  def handle_records(self, rows):
    counter = Counter()
    # student_id, room_type_id, lesson_id, begin_at
    for row in rows:
      room_type = row[1]
      lesson_count = float()
      if room_type == 0:
        lesson_count = 1
      elif room_type == 1:
        lesson_count = 1
      elif room_type == 2:
        lesson_count = 1.5
        if row[3].strftime('%Y-%m-%d') > '2018-03-19':
          lesson_count = 1
      elif room_type == 5:
        lesson_count = 1.8
      counter[row[0]] += lesson_count
    return counter

    

if __name__ == "__main__":
  #vip_columns = f.get_student_by_tag(1)
  ls = LessonStudent()
  m = MonthIndex('2018-01-01','2018-02-01')
  a = ls.get_records_by_month(m)

  #func = lambda x: 0 if x == 0 else 1
  #f = fb.filter(items = vip_columns)
  #f = f.applymap(func)
  #cb = c.get_dataframe()
  #c = cb.filter(items= vip_columns)
  #a = f.dot(c.T) 
