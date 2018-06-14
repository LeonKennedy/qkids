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

vip_filename = 'data/vip_student_series.pkl'
def data_frame_1(refresh=False):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe(refresh=refresh)
  f.create_vip_student_series()

# 1. 每个月的新长期用户  *   课消
def mission_1(refresh):
  f = FisrtBuyMonthStudent()
  fb = f.get_dataframe()
  vip_columns = pd.read_pickle(vip_filename)
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


def misson_7(refresh):
  pass
 
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
  #mission_4(False)
  #mission_5(False)
  mission_6(False)
  #my_mission_2_1()
  #my_mission_2_2()
  #data_frame_1(True)
