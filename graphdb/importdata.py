#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: importdata.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: ---
# @Create: 2018-06-29 10:31:46
# @Last Modified: 2018-06-29 10:31:46
#

import logging, sys, os, pdb
sys.path.append('..')
from neo4j.util import watch
from LocalDatabase import get_bills_connection, get_schedule_connection, get_product_connection, get_cash_billing_connection, get_course_connection, get_graphdb_deriver
from neo4j.v1 import GraphDatabase


class Room:

  def __init__(self):
    self.conn = get_schedule_connection()

  def get_records(self):
    sql = "select id, schedule_id, teacher_id, student_count, created_at, status \
      from rooms  where is_internal = 0 and deleted_at is null limit 100"
    with self.conn.cursor() as cur:
      pass


class Student:
  def __init__(self):
    self.conn = get_product_connection()
    self.driver = get_graphdb_deriver()

  def get_records(self):
    sql = "select user_id, nick_name, gender, birthday, place from users \
      where encrypt_mobile_v2 is not null and deleted_at is null limit 100"
    with self.conn.cursor() as cur:
      cur.execute(sql)
      while cur.rownumber < cur.rowcount:
        yield cur.fetchone()

  def insert(self):
    cql = "merge (a:Student {id: $uid, nick_name: $name, gender: $gender})"
    def add_student(tx, row):
      tx.run(cql, uid=row[0], name=row[1], gender=row[2])

    with self.driver.session() as session:
      for row in self.get_records():
        self.create_transaction(row)  
        #session.write_transaction()
  def create_transaction(row):
    k = ['id', 'name', 'gender', 'birthday', 'place']
    a = dict(k, row)
    pdb.set_trace()
    cql = "merge (a:Student {id:$uid}"
    cql += "on create set "
    
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
  s.insert()
