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
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection, get_graphdb_deriver
from neo4j.v1 import GraphDatabase


class ModelHandler:

  def get_records(self):
    with self.conn.cursor() as cur:
      cur.execute(self.sql)
      while cur.rownumber < cur.rowcount:
        yield cur.fetchone()

  def insert(self):
    with self.driver.session() as session:
      for row in self.get_records():
        a = self.create_transaction(row)  
        session.write_transaction(a)
    
class Room(ModelHandler):

  def __init__(self):
    self.conn = get_schedule_connection()
    self.driver = get_graphdb_deriver()

    self.sql = "select id, schedule_id, teacher_id, student_count, created_at, status \
      from rooms  where is_internal = 0 and deleted_at is null limit 100"

  def create_transaction(self, row):
    cql = "merge (a:Room {id:%d})" % row[0]
    param = "a.schedule_id=%d," % row[1]
    param += "" if row[2] is None else "a.teacher_id=%d," % row[2]
    param += "a.student_count=%d," % row[3]
    param += "a.created_at=datetime(%r)," % row[4].strftime('%Y-%m-%dT%H:%M:%S')
    param += "a.status = %d" % row[5]
    cql += " on create set %s" % param
    cql += " on match set %s" % param
    print(cql)
    return lambda tx:tx.run(cql)
    

class Student(ModelHandler):
  def __init__(self):
    self.conn = get_product_connection()
    self.driver = get_graphdb_deriver()

    self.sql = "select user_id, nick_name, gender, birthday, place from users \
      where encrypt_mobile_v2 is not null and deleted_at is null limit 100"


  def create_transaction(self, row):
    cql = "merge (a:Student {id:%d}) " % row[0]
    param = " a.name=%r ," % row[1]
    param += " a.gender=%d ," % row[2]
    param += "" if row[3] is None else " a.birth=date(%r)," % row[3].strftime('%Y-%m-%d')
    param += "" if row[4] is None else " a.place=%r" % row[4]
    cql += "on create set %s " %  param
    cql += "on match set %s " % param
    def add_student(tx):
      tx.run(cql)
    return add_student
    
    
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
  #s.insert()
  r = Room()
  r.insert()
