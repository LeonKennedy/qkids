#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: data_frame_manage.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 管理类 主要和任务相关
# @Create: 2018-05-31 16:04:25
# @Last Modified: 2018-05-31 16:04:25

from data_frame import ConsumeMonthStudent, FisrtBuyMonthStudent, LessonStudent
from produce_month_index import MonthIndexFactroy, MonthIndex
import pdb
from collections import Counter
import pandas as pd
import numpy as np

def data_frame_1(refresh=False):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe()

# 1. 每个月的新长期用户  *   课消
def mission_1(refresh):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe()
  vip_columns = pd.read_pickle('vip_student')
  #pd.to_pickle(vip_columns, 'vip_student')
  func = lambda x: 0 if x == 0 else 1
  f = fb.filter(items = vip_columns)
  f = f.applymap(func)

  c = ConsumeMonthStudent(statistics_type='count')
  cb = c.get_dataframe(refresh=refresh)
  c = cb.filter(items= vip_columns)
  a = f.dot(c.T) 
  count_student = f.sum(axis= 1)
  a.insert(0, 'count', count_student)
  a.to_csv('data/student_concumser.csv')

# 2. 每个月的新长期用户  *   留存 只算一次
def mission_2(refresh):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe()
  vip_columns = pd.read_pickle('vip_student')
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
  fvip = f.get_student_by_tag(1)
  func = lambda x: 0 if x == 0 else 1
  f = fb.filter(items = fvip)
  f = f.applymap(func)

  ls = LessonStudent()
  m = MonthIndexFactroy(begin='2017-05')
  lsb = ls.get_dataframe(m, refresh=refresh)
  course_matrix = ls.aggregate_by_course()
  a = f.dot(course_matrix)
  a.to_csv('data/lesson_student.csv')
  print(a)
 
def my_mission_1():
  ls = LessonStudent()
  m = MonthIndexFactroy(begin='2017-05')
  lsb = ls.get_dataframe()
  u,s,v = np.linalg.svd(lsb.iloc[:,0:10000].values, False)
  pdb.set_trace()
  print(u)

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
  #mission_1(False)
  #mission_2(False)
  #mission_3(False)
  #my_mission_2_1()
  #my_mission_2_2()
  data_frame_1()
