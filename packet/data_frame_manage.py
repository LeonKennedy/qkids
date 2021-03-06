#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: data_frame_manage.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 管理类 主要和任务相关
# @Create: 2018-05-31 16:04:25
# @Last Modified: 2018-05-31 16:04:25

import sys, pdb
sys.path.append('..')
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection
from data_frame import ConsumeMonthStudent, FisrtBuyMonthStudent, LessonStudent, AppointmentStudent, FirstBuyMonthStudent, BillingMonthStudent, RefundDataFrame
from produce_month_index import MonthIndexFactroy, MonthIndex
from sklearn.decomposition import NMF, LatentDirichletAllocation
from collections import Counter
from time import time
import pandas as pd
import numpy as np

vip_filename = 'data/vip_student.pkl'
def data_frame_1(refresh=False):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe(refresh=refresh)
  f.create_vip_student_series()

# 1. 每个月的新长期用户  *   课消
def mission_1(refresh):
  f = FisrtBuyMonthStudent(3, 'distinct')
  fb = f.get_dataframe(refresh=True)
  #vip_columns = pd.read_pickle(vip_filename)
  #pd.to_pickle(vip_columns, 'vip_student')
  func = lambda x: 0 if x == 0 else 1
  #f = fb.filter(items = vip_columns)
  #f = f.applymap(func)

  c = ConsumeMonthStudent(3)
  cb = c.get_dataframe(refresh=refresh)
  #c = cb.filter(items= vip_columns)
  a = fb.dot(cb.T) 
  count_student = fb.sum(axis= 1)
  a.insert(0, 'count', count_student)
  pdb.set_trace()
  a.to_csv('data/student_consumser.csv')

# 2. 每个月的新长期用户  *   留存 只算一次
def mission_2(refresh):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe()
  vip_columns = pd.read_pickle(vip_filename)
  #pd.to_pickle(vip_columns, 'vip_student')
  func = lambda x: 0 if x == 0 else 1
  f = fb.filter(items = vip_columns)
  f = f.applymap(func)

  c = ConsumeMonthStudent(statistics_type='count')
  cb = c.get_dataframe(refresh=False)
  c = cb.filter(items= vip_columns)
  c = c.applymap(func)
  a = f.dot(c.T) 
  count_student = f.sum(axis= 1)
  a.insert(0, 'count', count_student)
  a.to_csv('data/student_retention.csv')

# 3.Lesson与Student 分布i
def mission_3(refresh):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe(refresh=refresh)
  vip_columns = pd.read_pickle(vip_filename)
  func = lambda x: 0 if x == 0 else 1
  f = fb.filter(items = vip_columns)
  f = f.applymap(func)

  ls = LessonStudent()
  m = MonthIndexFactroy(begin='2017-05')
  lsb = ls.get_dataframe(m, refresh=True)
  course_matrix = ls.aggregate_by_course()
  a = f.dot(course_matrix)
  a.to_csv('data/lesson_student.csv')

# 4.体课/新签人数/购买活动包  首次购买正价包
# 当月新签用户  *  购买正价课程包首单人数（当月购买人数）
def mission_4(refresh):
  f = FisrtBuyMonthStudent(category=2, statistics_type='distinct')
  fb = f.get_dataframe(refresh=refresh)
  fv = FisrtBuyMonthStudent(category=1, statistics_type='distinct')
  fvb = fv.get_dataframe(refresh=refresh)
  func = lambda x: 0 if x == 0 else 1
  f = fb.applymap(func)
  fv = fvb.applymap(func)
  a = f.dot(fv.T)
  count_student = f.sum(axis= 1)
  a.insert(0, 'count', count_student)
  pdb.set_trace()
  a.to_csv('data/experience_student_format_buy.csv')
  
# 5.购买正价包  在次购买正价包
def mission_5(refresh):
  f = FisrtBuyMonthStudent(category=1, statistics_type='count')
  fb = f.get_dataframe(refresh=refresh)
  s1 = pd.DataFrame(0, index = fb.index, columns = (0,), dtype='uint8')
  s2 = pd.DataFrame(0, index = fb.index, columns = (0,), dtype='uint8')
  s3 = pd.DataFrame(0, index = fb.index, columns = (0,), dtype='uint8')
  for student in fb.iteritems():
    times = 0
    for c in student[1].items():
      if c[1] > 0:
        if times == 0:
          s1.insert(s1.columns.size, student[0] , 0)
          s2.insert(s2.columns.size, student[0] , 0)
          s3.insert(s3.columns.size, student[0] , 0)
          s1.loc[c[0], student[0]] = 1
        elif times == 1:
          s2.loc[c[0], student[0]] = 1
        elif times == 2:
          s3.loc[c[0], student[0]] = 1
          continue
        times += 1
  a = s1.dot(s2.T)
  a.insert(0, 'count', s1.sum(axis=1))
  a.to_csv('data/student_format_rebuy.csv')
  b = s1.dot(s3.T)
  b.insert(0, 'count', s1.sum(axis=1))
  b.to_csv('data/student_format_rebuy_2.csv')
  return s1, s2, s3

# 6.购买正价包  在次购买活动课包
def mission_6(refresh):
  f = FisrtBuyMonthStudent(category=1, statistics_type='distinct')
  fb = f.get_dataframe(refresh=refresh)
  func = lambda x: 0 if x == 0 else 1
  fb = fb.applymap(func)
  fs = FisrtBuyMonthStudent(category=2, statistics_type='count')
  fsb = fs.get_dataframe(refresh=refresh)
  pdb.set_trace()
  a = fb.dot(fsb.T)
  count_student = fb.sum(axis= 1)
  a.insert(0, 'count', count_student)
  a.to_csv('data/format_stduent_experience_buy.csv')

# 7 正价课首单 *  发生退费行为
def mission_7(refresh):
  month_list = ['2015-12', '2016-01', '2016-02', '2016-03','2017-03','2016-04','2016-05','2016-06','2016-07','2016-08',
  '2016-09','2016-10','2016-11', '2016-12','2017-01','2017-02','2017-03', '2017-04','2017-05','2017-06','2017-07',
  '2017-08','2017-09','2017-10','2017-11', '2017-12', '2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06']
  df = pd.DataFrame(0, index = month_list, columns = month_list, dtype='uint8')
  student_set = set()
  bills = dict()

  conn = get_bills_connection()
  with conn.cursor() as cur:
    sql = "select id, student_id, paid_at from bills where product_id \
    in (5,6,13,19) and status in (20, 70, 80) and student_id > 0 and deleted_at \
    is null"
    cur.execute(sql)
    print(cur.rowcount)
    while cur.rownumber < cur.rowcount:
      r = cur.fetchone()
      if r[1] not in student_set:
        student_set.add(r[1])
        month = r[2].strftime('%Y-%m')
        if month in bills.keys():
          bills[month].append(r[0])
        else:
          bills[month] = [r[0],]

  with conn.cursor() as cur:
    sql = "select bill_id, updated_at from refunds where status = 3 and type = 0 and deleted_at is null"
    cur.execute(sql)
    print(cur.rowcount)
    while cur.rownumber < cur.rowcount:
      r = cur.fetchone()
      for m,ml in bills.items():
        if r[0] in ml:
          df.loc[m, r[1].strftime('%Y-%m')] += 1

  conn = get_product_connection()
  with conn.cursor() as cur:
    sql = "select bill_id, updated_at from refund where deleted_at is null and status_id = 20 "
    cur.execute(sql)
    print(cur.rowcount)
    while cur.rownumber < cur.rowcount:
      r = cur.fetchone()
      for m,ml in bills.items():
        if r[0] in ml:
          df.loc[m, r[1].strftime('%Y-%m')] += 1

  cash_bills = dict()
  conn = get_cash_billing_connection() 

  with conn.cursor() as cur: 
    sql = "select id, student_id, paid_at from bills where product_id in (2,3,4) and \
    status in  (20, 70, 80) and deleted_at is null"
    cur.execute(sql)
    print(cur.rowcount)
    while cur.rownumber < cur.rowcount:
      r = cur.fetchone()
      if r[1] not in student_set:
        student_set.add(r[1])
        month = r[2].strftime('%Y-%m')
        if month in cash_bills.keys():
          cash_bills[month].append(r[0])
        else:
          cash_bills[month] = [r[0],]

  with conn.cursor() as cur: 
    sql = "select bill_id, updated_at from refunds where status = 3 and type = 0 and deleted_at is null"
    cur.execute(sql)
    print(cur.rowcount)
    while cur.rownumber < cur.rowcount:
      r = cur.fetchone()
      for m,ml in cash_bills.items():
        if r[0] in ml:
          df.loc[m, r[1].strftime('%Y-%m')] += 1

  print(df) 
  df.insert(0, 'count', 0)
  for m in month_list:
    lb = len(bills[m]) if m in bills.keys() else 0
    lcb = len(cash_bills[m]) if m in cash_bills.keys() else 0 
    df.loc[m, 'count'] = lb + lcb
  df.to_csv('data/first_format_bills_refund.csv') 


# 每个月的订单 * 每个订单的消费记录
def mission_8(refresh):
  month_list = ['2016-01', '2016-02', '2016-03','2017-03','2016-04','2016-05','2016-06','2016-07','2016-08',
  '2016-09','2016-10','2016-11', '2016-12','2017-01','2017-02','2017-03', '2017-04','2017-05','2017-06','2017-07',
  '2017-08','2017-09','2017-10','2017-11', '2017-12', '2018-01', '2018-02', '2018-03', '2018-04', '2018-05', '2018-06']
  df = pd.DataFrame(0, index = month_list, columns = month_list, dtype='uint8')
  student_set = set()
  bills = dict()
  with conn.cursor() as cur:
    sql = "select id, student_id, paid_at from bills where paid_at > '2016-01-01' and product_id \
    in (5,6,13,19, 4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 27,28, 29, 1003, 1004, 1005, 1006, \
    1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032, 1033, 1034, 1035, 1036, 1037, 1038) \
    and status in (20, 70, 80) and student_id > 0 and deleted_at is null"
    cur.execute(sql)
    print(cur.rowcount)
    while cur.rownumber < cur.rowcount:
      r = cur.fetchone()
      if r[1] not in student_set:
        student_set.add(r[1])
        month = r[2].strftime('%Y-%m')
        if month in bills.keys():
          bills[month].append(r[0])
        else:
          bills[month] = [r[0],]

# （新用户小单，新用户大单，续费小单，续费大单）*  每个月人数
def mission_9(refresh):
  f = FisrtBuyMonthStudent(category=1, statistics_type='count')
  ms = MonthIndexFactroy(begin='2015-12')
  buy_set = set()
  df = pd.Series(0, index = ms.index, dtype='uint16')
  for m in ms.output:
    for row in f.get_student_by_month(m):
      sid = row[0] 
      if sid not in buy_set:
        buy_set.add(sid)
        df.loc[m.name] += 1
    print(df)

# 月  * 【 体验课积累，体验课新增，体验课流水，正式课累计，正式新增，正式流水】
def mission_10(refresh):
  f = FisrtBuyMonthStudent(category= 3)
  ms = MonthIndexFactroy()
  buy_set = set()
  format_buy_set = set()
  index = ['experience','experience_flow', 'format', 'format_flow']
  df = pd.DataFrame(0, index = ms.index, columns = index, dtype='uint32')
  for m in ms.output:
    print(m.name)
    for row  in f.get_records_by_month(m):
      pdb.set_trace()
      sid = row[0]
      pid = row[2]
      # bill
      if row[1] == 0 :
        if pid in (5,6,13, 19) :
          if sid not in format_buy_set:
            df.loc[m.name, 'format'] += 1
            df.loc[m.name, 'format_flow'] += int(row[3])
            format_buy_set.add(sid)
        else:
          if sid not in buy_set:
            df.loc[m.name, 'experience'] += 1
            df.loc[m.name, 'experience_flow'] += int(row[3])
            buy_set.add(sid)
      # cash bill
      elif row[1] == 1:
        if pid == 1:
          if sid not in buy_set:
            df.loc[m.name, 'experience'] += 1
            df.loc[m.name, 'experience_flow'] += int(row[3])
            buy_set.add(sid)
        elif pid in (2,3,4):
          if sid not in format_buy_set:
            df.loc[m.name, 'format'] += 1
            df.loc[m.name, 'format_flow'] += int(row[3])
            format_buy_set.add(sid)
  pdb.set_trace()
  print(df)
  cumsum_format = df.loc[:, 'format'].cumsum()
  cumsum_experience = df.loc[:, 'experience'].cumsum()
  df.insert(2, 'cumsum_format',  cumsum_format)
  df.insert(0, 'cumsum_experience',  cumsum_experience)
  df.to_csv('data/user_recharge.csv')

# 月  * 【 体验课积累，体验课新增课时，正式积累，正式新增课时】
def mission_10_1(refresh):
  ms = MonthIndexFactroy()
  buy_set = set()
  format_buy_set = set()
  index = ['experience', 'format' ]
  df = pd.DataFrame(0, index = ms.index, columns = index, dtype='float32')
  b = BillingMonthStudent(1)
  for m in ms.output:
    for row in b.get_lesson_records_by_month(m):
      sid = row[0]
      lesson = row[1]
      if sid not in format_buy_set:
        df.loc[m.name, 'format'] += round(float(lesson), 1)
        format_buy_set.add(sid)
  cumsum_format = df.loc[:, 'format'].cumsum()
  df.insert(1, 'cumsum_format',  cumsum_format)
  b = BillingMonthStudent(2)
  for m in ms.output:
    for row in b.get_lesson_records_by_month(m):
      sid = row[0]
      lesson = row[1]
      if sid not in buy_set:
        df.loc[m.name, 'experience'] += float(round(lesson, 1))
        buy_set.add(sid)
  cumsum_experience = df.loc[:, 'experience'].cumsum()
  df.insert(0, 'cumsum_experience',  cumsum_experience)
  df.to_csv('data/user_recharge_lessons.csv')
  print(df)

# 月  * 【 体验课，体验课总流水，正式，正式流水】
def mission_10_2(refresh):
  ms = MonthIndexFactroy(begin='2017-08')
  index = ['experience', 'experience_flow', 'format', 'format_flow' ]
  df = pd.DataFrame(0, index = ms.index, columns = index, dtype='float32')
  b = BillingMonthStudent(1)
  for m in ms.output:
    for row in b.get_lesson_gift_by_month(m):
      lesson = row[1]
      df.loc[m.name, 'format'] += round(float(lesson), 1)
    for row in b.get_money_records_by_month(m):
      df.loc[m.name, 'format_flow'] += int(row[3])
  b = BillingMonthStudent(2)
  for m in ms.output:
    for row in b.get_lesson_records_by_month(m):
      lesson = row[1]
      df.loc[m.name, 'experience'] += round(float(lesson), 1)
    for row in b.get_money_records_by_month(m):
      df.loc[m.name, 'experience_flow'] += int(row[3])
  cumsum = df.loc[:, 'format_flow'].cumsum()
  df.insert(3, 'cumsum_format_flow',  cumsum)
  cumsum = df.loc[:, 'format'].cumsum()
  df.insert(2, 'cumsum_format',  cumsum)
  cumsum_experience = df.loc[:, 'experience_flow'].cumsum()
  df.insert(1, 'cumsum_experience_flow',  cumsum_experience)
  cumsum_experience = df.loc[:, 'experience'].cumsum()
  df.insert(0, 'cumsum_experience',  cumsum_experience)
  df.to_csv('data/user_recharge_lessons_flow.csv')



# 月新增用户 * 每个月的课时消耗情况
def mission_11(refresh):
  f = FirstBuyMonthStudent(1)
  fb = f.get_dataframe(refresh)
  a = AppointmentStudent()
  a.set_student_list(fb.columns)
  cb = a.get_dataframe(True)
  #c = cb.filter(items= vip_columns)
  a = fb.dot(cb.T) 
  count_student = fb.sum(axis= 1)
  a.insert(0, 'count', count_student)
  pdb.set_trace()
  a.to_csv('data/student_consumser.csv')

def mission_12(refresh):
  #ss = get_vip_student_set()
  f = FirstBuyMonthStudent(1)
  fb = f.get_dataframe()
  b = BillingMonthStudent(3)
  b.set_student_list(fb.columns)
  bb = b.get_dataframe()
  r = RefundDataFrame(3)
  r.set_student_list(fb.columns)
  rb = r.get_dataframe()
  sub = bb-rb
  a = fb.dot(sub.T)
  a.insert(0, 'count-refund', a.sum(axis=1))
  #a.insert(0, 'count', bb.sum(axis=1)) 
  pdb.set_trace()
  a = a.round()
  a.to_csv('data/student_buy_lessons_sub_refunds.csv')
  
  print(a)

def my_mission_1():
  ls = LessonStudent()
  m = MonthIndexFactroy(begin='2018-03')
  lsb = ls.get_dataframe()
  #ls.scan_records(m)
  lda = LatentDirichletAllocation(n_components=12, max_iter=5, learning_method='online', learning_offset=50., random_state=0)
  t0 = time()
  lda.fit(lsb)
  print("done in %0.3fs." % (time() - t0))
  #u,s,v = np.linalg.svd(lsb.iloc[:,0:10000].values, False)
  for topic_idx, topic in enumerate(lda.components_):
    print("Topic #%d:" % topic_idx)
    print(" ".join([ str(lsb.columns[i]) for i in topic.argsort()[-20:]]))
    print()

# 上课统计
def my_mission_2_1():
  c = ConsumeMonthStudent(statistics_type='count')
  ms = MonthIndexFactroy(end='2017-01')
  di = pd.date_range('20151201', '20170131', freq='D')
  #di = pd.date_range('20170201', '20180606', freq='D')
  s = pd.Series(0, index=di)
  for m in ms.output:
    print(m.name)

    for row in c.get_records_by_month(m):
      s.loc[row[2]] += 1
  pd.to_pickle(s, 'day_consum.pkl')
  
 
#每日消耗
def my_mission_2_2():
  c = ConsumeMonthStudent(statistics_type='count')
  ms = MonthIndexFactroy(begin='2017-02')
  #di = pd.date_range('20170201', '20180609', freq='D')
  #di = pd.date_range('20170201', '20180606', freq='D')
  #s = pd.Series(0, index=di)
  filename = 'day_consum_2016.pkl'
  s = pd.read_pickle(filename)
  for m in ms.output:
    print(m.name)
    counter = Counter()
    for row in c.get_records_by_month(m):
      counter[row[2]] += 1
    for k,v in counter.items():
      s.loc[k] = v
    pd.to_pickle(s, filename)


if __name__ == "__main__":
  data_frame_1(True)
  mission_10_2(True)
  mission_2(True)
  mission_3(True)
  mission_4(True)
  mission_5(True)
  mission_6(True)
  mission_7(True)
  mission_9(True)
  mission_10(True)
  #my_mission_1()
  #my_mission_2_2()
