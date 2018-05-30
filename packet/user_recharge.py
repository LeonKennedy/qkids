#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: user_recharge.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: 用户充值
#               累计体验课用户数 ， 新增体验课用户数， 累计长期课用户数， 新增长期课用户数（老用户续费不算）， 新用户流水
# @Create: 2018-05-28 21:19:13
# @Last Modified: 2018-05-28 21:19:13

import sys, pdb, csv
sys.path.append('..')
from LocalDatabase import get_bills_connection
from produce_month_index import MonthIndexFactroy

class UserRecharge:

  bill_100_students = set()
  bill_students = set()

  def __init__(self):
    self.conn = get_bills_connection()
    self.cur = self.conn.cursor()
    self.output_file = 'data/user_recharge.csv'

    self.small_product_id = (4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 29, 
    1003, 1004, 1005, 1006, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
    1033, 1034, 1035, 1036, 1037, 1038)
    self.big_product_id = (5, 6, 13, 19)
    self.all_product_id = self.big_product_id + self.small_product_id

    self.month_index = MonthIndexFactroy('2018-03')

  def query_month(self, m):
    print(m)
    sql = "select id, product_id, student_id, price, actual_price, paid_at \
      from bills where paid_at is not null and \
      paid_at between %r and %r and product_id in %s and deleted_at is \
      null and status in (20, 80) " % (m.begin, m.end, str(self.all_product_id))
    self.cur.execute(sql)

    s1 = set()
    s2 = set()
    price = float()
    actual_price = float()
    for row in self.cur.fetchall():
      if row[1] in self.small_product_id:
        s1.add(row[2])
      else:
        s2.add(row[2])
        price += float(row[3])
        actual_price += float(row[4])
    return s1, s2, price, actual_price

  def combin(self):
    output = list()
    for m in self.month_index.output:
      s1, s2, price , aprice = self.query_month(m)
      self.bill_100_students.update(s1)
      total_100 = len(self.bill_100_students)
      new_100 = len(s1)
      
      new_bill_students = s2 - self.bill_students
      new_bill = len(new_bill_students)
      bill = len(self.bill_students)
      self.bill_students.update(s2)
      print('%d, %d, %d, %d, %f, %.4f' % (total_100, new_100, bill, new_bill, price , aprice))
      output.append((m.name, total_100, new_100, 100 * new_100, bill, new_bill, price , aprice))
    self.save(output)

  def save(self, data):
    with open(self.output_file, 'w', newline='') as csvfile:
      spamwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL) 
      spamwriter.writerow(['month','accumulative experience', 
        'fresh experience', 'experience flow', 'accumulative loyal', 'fresh loyal', 'loyal flow', 'loyal actual flow' ])
      for row in data:
        spamwriter.writerow(row)

if __name__ == "__main__":
  u = UserRecharge()
  u.combin()
  #print(u.get_month_range())
      
