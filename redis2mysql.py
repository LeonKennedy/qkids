#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: redis2mysql.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 将redis的数据抽取放入mysql
# @Create: 2018-05-14 16:52:44
# @Last Modified: 2018-05-14 16:52:44
#
import redis, pdb
from LocalDatabase import get_statistics_database_connection, get_user_connection
from QkidsRedis import getCentral as get_central_redis

class DBRC:
  
  central_redis = None
  def __init__(self):
    self.central_redis = get_central_redis()
    self.conn = get_statistics_database_connection()
    self.user_conn = get_user_connection()

  def transfer(self, key, tablename):
    cur = self.conn.cursor()
    for i in self.central_redis.smembers(key):
      sql = 'insert into %s (student_id, tag_id) value(%s, 201)' % (tablename, int(i))
      cur.execute(sql)
    cur.close()
    self.conn.commit()

  def run(self):
    r = self.central_redis
    a = r.keys("Student:*:Tags")
    print(len(a))
    count = 0
    for i in a:
      _,c,_ = i.decode().split(':')
      self.save_student_tags(int(c))
      count += 1
      if count % 1000 == 0:
        print('have finish %d student' % count)

  def save_student_tags(self, student_id):
    r = self.central_redis
    key = "Student:%d:Tags" % student_id
    self.cur = self.conn.cursor()
    for i in r.smembers(key):
      self.select_or_insert('student_tags', {'student_id':student_id, 'tag_id': int(i)})
    self.cur.close()
    #self.conn.commit()

  def select_or_insert(self, tablename, params):
    params_str = "and".join([" %s = %r " %(k, v) for k,v in params.items() ])
    select_sql = "select id from %s where %s" % ( tablename, params_str)
    self.cur.execute(select_sql)
    if self.cur.fetchone():
      return 2
    else:
      lenght = len(params)
      columns = ','.join([ i for i in params.keys()])
      values = str(tuple([i for i in params.values()]))
      insert_sql = "insert into %s (%s) values %s" % (tablename, columns, values)
      self.cur.execute(insert_sql)
      return 1

  # 增量更新
  def increase_tags(self):
    for tag in self.get_increate_tags():
      print('save tag: %d '% tag)
      self.save_tag_student(tag)
   
  def get_increate_tags(self):
    have_tags = None
    tags = None
    with self.conn.cursor() as cur:
      sql = "select distinct tag_id from student_tags"
      cur.execute(sql)
      have_tags = set([ i[0] for i in cur.fetchall() ])

    with self.user_conn.cursor() as cur:
      sql = "select id from tags"
      cur.execute(sql)
      tags = set([ i[0] for i in cur.fetchall() ])

    return tags - have_tags

  def save_tag_student(self,tag):
    r = self.central_redis
    key = "Tag:%d:Students" % tag
    self.cur = self.conn.cursor()
    students = r.smembers(key)
    print('find %d students' % len(students))
    for i in students:
      self.select_or_insert('student_tags', {'student_id':int(i), 'tag_id': tag})
    self.cur.close()
    self.conn.commit()

if __name__ == "__main__":
  d = DBRC()
  #d.transfer("Tag:%d:Students" % 201, 'student_tags')
  #d.run()
  d.increase_tags()
  #d.save_tag_student(277)



