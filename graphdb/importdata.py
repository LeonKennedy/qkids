#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: importdata.py
# @Author: Lorenzo - lionhe0119@hotmail.com
# @Description: ---
# @Create: 2018-06-29 10:31:46
# @Last Modified: 2018-06-29 10:31:46
#

import logging, sys, os, pdb
sys.path.append('..')
from neo4j.util import watch
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection, get_graphdb_deriver, get_teacher_connection
from neo4j.v1 import GraphDatabase


class ModelHandler:

  def get_records(self):
    with self.conn.cursor() as cur:
      cur.execute(self.sql)
      print('finish execute')
      while cur.rownumber < cur.rowcount:
        yield cur.fetchone()

  def run(self):
    with self.driver.session() as session:
      for row in self.get_records():
        a = self.create_transaction(row)  
        session.write_transaction(a)

  def create_transaction(self, row):
    cql = self.get_cypherql(row)
    return lambda tx:tx.run(cql)


class Schedule(ModelHandler):
  def __init__(self):
    self.conn = get_schedule_connection()
    self.driver = get_graphdb_deriver()
    self.sql = "select id, room_type_id, room_count, course_id,lesson_id,begin_at, \
      created_at from schedules where begin_at > '2015-12-01' and deleted_at is null \
      and created_at is not null"

  def get_cypherql(self, row):
    cql = "merge (a:Schedule {id:%d})" % row[0]
    param = "a.room_type_id=%d," % row[1]
    param += "a.room_count=%d," % row[2]
    param += "a.course_id=%d," % row[3]
    param += "a.lesson_id=%d," % row[4]
    param += "a.begin_at=datetime(%r)," % row[5].strftime('%Y-%m-%dT%H:%M:%S') 
    param += "a.created_at = datetime(%r)," % row[6].strftime('%Y-%m-%dT%H:%M:%S')
    param += "a.updated_at = timestamp()"
    cql += " on create set %s" % param
    cql += " on match set %s" % param
    return cql
    
class Room(ModelHandler):

  def __init__(self):
    self.conn = get_schedule_connection()
    self.driver = get_graphdb_deriver()


  def init_insert(self):
    self.sql = "select id, schedule_id, teacher_id, student_count, created_at, status \
      from rooms  where is_internal = 0 and status in (0,1) and deleted_at is null"
    self.get_cypherql = self.get_insert_cypherql

  def init_relation_schedule(self):
    self.sql = "select id, schedule_id from rooms where rooms.created_at > \
      '2018-01-01' and is_internal = 0 and \
      status in (0, 1) and deleted_at is null "
    self.get_cypherql = self.get_relation_schedule_cypherql

  def get_relation_schedule_cypherql(self, row):
    cql = "match (r:Room {id:%d} ), (s:Schedule {id:%d}) " % (row[0], row[1])
    cql += "merge (r)-[:PART_OF]->(s) return s.id"
    return cql
    
  def get_insert_cypherql(self, row):
    cql = "merge (a:Room {id:%d})" % row[0]
    param = "a.schedule_id=%d," % row[1]
    param += "" if row[2] is None else "a.teacher_id=%d," % row[2]
    param += "a.student_count=%d," % row[3]
    param += "a.created_at=datetime(%r)," % row[4].strftime('%Y-%m-%dT%H:%M:%S')
    param += "a.status = %d" % row[5]
    cql += " on create set %s" % param
    cql += " on match set %s" % param
    return cql
    

class Student(ModelHandler):
  def __init__(self):
    self.driver = get_graphdb_deriver()


  def init_insert(self):
    self.conn = get_product_connection()
    self.sql = "select user_id, nick_name, gender, birthday, place, vip, first_large_buy_at from users \
      where user_id > 125144 and encrypt_mobile_v2 is not null and deleted_at is null \
      and birthday > '1988-01-01'"
    self.get_cypherql = self.get_insert_cypherql
    
  def init_relation_room(self):
    self.conn = get_schedule_connection()
    self.sql = "select id, student_id, room_id, status, created_at from student_appointments \
      where created_at > '2018-01-01' and status in (0,1,3,4) and deleted_at is null"
    self.get_cypherql = self.get_relation_room_cypherql

  def get_relation_room_cypherql(self, row):
    cql = "match (r:Room {id:%d}), (s:Student {id:%d}) " % (row[2], row[1])
    cql += "merge (r)<-[:JOIN {id:%d}]-(s)" % row[0]
    return cql

  def get_insert_cypherql(self, row):
    cql = "merge (a:Student {id:%d}) " % row[0]
    param = " a.name=%r ," % row[1]
    param += " a.gender=%d ," % row[2]
    param += "" if row[3] is None else " a.birth=date(%r)," % row[3].strftime('%Y-%m-%d')
    param += "" if row[4] is None else " a.place=%r," % row[4]
    param += " a.vip = %d," % row[5]
    param += '' if row[6] is None else " a.first_large_buy_at = datetime(%r)," % row[6].strftime('%Y-%m-%dT%H:%M:%S')
    param += "a.updated_at = timestamp()"
    cql += "on create set %s " %  param
    cql += "on match set %s " % param
    return cql

class Teacher(ModelHandler):
  def __init__(self):
    self.conn = get_teacher_connection()
    self.driver = get_graphdb_deriver()
    self.sql = "select id, nick_name, gender, status_id, avg_rating, likes from teachers \
      where status_id in (1,2,5) and deleted_at is null"

  def get_cypherql(self, row):
    cql = "merge (a:Teacher {id:%d}) " % row[0]
    param = " a.name = %r," % row[1]
    param += " a.gender=%d," % row[2]
    param += " a.status = %d," % row[3]
    param += " a.status = %f," % float(row[4])
    param += " a.likes = %d " % row[5]
    cql += "on create set %s " %  param
    cql += "on match set %s " % param
    return cql

def test_driver():
  driver = get_graphdb_deriver()
  def print_count(tx):
    cql = "match (n) return count(n)"
    for record in tx.run(cql):
      print(record)

  with driver.session() as session:
    session.read_transaction(print_count)

if __name__ == "__main__":
  #test_driver()
  s = Student()
  #s.init_insert()
  s.init_relation_room()
  s.run()
  #r = Room()
  #r.init_relation_schedule()
  #r.run()
  #sh = Schedule()
  #sh.insert()
