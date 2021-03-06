#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: student_consume.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 新增用户的消耗
# @Create: 2018-05-29 10:00:00
# @Last Modified: 2018-05-29 10:00:00

import sys, pdb
sys.path.append('..')
import pandas as pd
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection
from produce_month_index import MonthIndexFactroy, MonthIndex

class StudentConsumeStatistics:

  def __init__(self):
    months = MonthIndexFactroy('2018-05')
    self.bill_conn = get_bills_connection()
    self.cash_bill_conn = get_cash_billing_connection()
    self.legacy_conn = get_product_connection()
    self.students = dict()
    self.all_students = set()
    self.months = months

    self.user_df =  pd.DataFrame(0, index = months.index, columns = ['increase_student'] + months.index)
    self.consumer_df =  pd.DataFrame(0, index = months.index, columns = ['increase_student'] + months.index)
    self.nil_students()

  # 用户分组
  # 新增用户安月分
  def nil_students(self):
    print('split student')
    for m in self.months.output:
      self.students[m.name] = self.get_student_by_month_2(m)
      self.user_df.at[m.name,'increase_student'] = len(self.students[m.name])
      self.consumer_df.at[m.name,'increase_student'] = len(self.students[m.name])
      self.all_students.update(self.students[m.name])


  def get_student_by_month_2(self, month):
    with self.legacy_conn.cursor() as cur:
      sql = "select user_id from users where first_large_buy_at between %r and %r and \
      deleted_at is null" % (month.begin, month.end)
      cur.execute(sql)
      return set([ i[0] for i in cur.fetchall()])

  def get_student_by_month(self, month):
    with self.bill_conn.cursor() as cur:
      sql = "select distinct student_id from bills where paid_at between %r and %r \
      and product_id in (5,6, 13, 19)  and status in (20, 80) and deleted_at is null" % (month.begin, month.end)
      cur.execute(sql)
      data = set([ i[0] for i in cur.fetchall()])
      return data
    
  def get_student_comsumption(self, m):
    if m.name < '2017-08':
      with self.legacy_conn.cursor() as cur:
        sql = "select user_id, lesson_count from consumes where created_at  \
        between %r and %r and status_id = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        return cur.fetchall()
    elif m.name == '2017-08':
      legacy_data = list()
      new_data = None
      with self.legacy_conn.cursor() as cur:
        sql = "select user_id, lesson_count from consumes where created_at  \
        between %r and %r and status_id = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        legacy_data = list(cur.fetchall())
      with self.bill_conn.cursor() as cur:
        sql  = "select student_id, lesson_count from student_consumptions use \
        index(created_at_idx) where created_at between %r and %r and status = 2 \
        and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        new_data = cur.fetchall()
      legacy_data.extend(new_data)
      return legacy_data
    else:
      with self.bill_conn.cursor() as cur:
        sql  = "select student_id, lesson_count from student_consumptions use \
        index(created_at_idx) where created_at between %r and %r and \
        status = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        data = cur.fetchall()
      if m.name < '2018-03':
        return data
      with self.cash_bill_conn.cursor() as cur:
        sql = "select student_id, case room_type when 2 then 1.5 when 5 then 2.5 \
        else 3 end from student_consumptions where created_at between %r and %r and \
        status = 2 and deleted_at is null " % (m.begin, m.end)
        cur.execute(sql)
        return data + cur.fetchall() 

  def agg_data(self, data):
    student = dict()
    for row in data:
      student[row[0]] = float(student[row[0]])+ float(row[1]) if row[0] in student.keys() else float(row[1])
    return student

  def run(self):
    print('fetch records and scan')
    for m in self.months.output:
      data = self.get_student_comsumption(m)
      self.scan_transfer(data, m)

  def scan_transfer(self, data, month):
    student_set = set()
    for k,v in data:
      month_index = self.get_month_index_by_stduent(k)
      if not month_index is None:
        self.consumer_df.at[month_index, month.name] += 1
        if not k in student_set:
          self.user_df.at[month_index, month.name] += 1
      student_set.add(k)

  def save(self):
    dire  = 'data/'
    self.consumer_df.to_csv(dire + 'student_consumer.csv')
    self.user_df.to_csv(dire + 'student_retention.csv')

  def get_month_index_by_stduent(self, sid):
    for i,v in self.students.items():
      if sid in v:
        return i

if __name__ == "__main__":
  #sf = StudentFirstBuy()
  #sf.nil_students()
  sc = StudentConsumeStatistics()
  #m  = MonthIndex('2017-08-01', '2017-09-01')
  #a = sc.get_student_comsumption(m)
  #sc.agg_data(a)
  sc.run()
  sc.save()
  print(sc.consumer_df)

