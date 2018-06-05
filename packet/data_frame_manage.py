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
import pandas as pd
import numpy as np


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


if __name__ == "__main__":
  #mission_1(False)
  #mission_2(False)
  #mission_3(False)
  my_mission_1()



  




