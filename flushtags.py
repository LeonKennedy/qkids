#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: flushtags.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 刷新用户的tag
# @Create: 2018-05-16 14:09:07
# @Last Modified: 2018-05-16 14:09:07

import argparse, csv, redis
from QkidsRedis import getCentral as get_central_redis

class StudentTags:

  tag_key = 'Tag:%d:Students'
  student_key = 'Student:%d:Tags'

  def __init__(self):
    self.central_redis = get_central_redis()

  def flush(self, users, tag_id):
    print("student count %d" % len(users))
    r =  self.central_redis
    key = self.tag_key % int(tag_id)
    r.sadd(key, *users)
    for student_id in users:
      key = self.student_key % student_id
      r.sadd(key, tag_id)

  def flush_student(self, user_id, tag_id):
    key = self.student_key % int(user_id)

  def read_csv(self, path):
    with open(path) as f:
      reader = csv.reader(f)
      next(reader)
      return [ int(i[0]) for i in reader ]

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument('file_path', default=None, type=str, help='CSV file pathe')
  parser.add_argument('tag_id', default=None, type=int, help='CSV file pathe')
  args = parser.parse_args()
  if args.file_path and args.tag_id:
    st = StudentTags()
    students = st.read_csv(args.file_path)
    st.flush(students[1:], args.tag_id)
  else:
    parser.print_help()
