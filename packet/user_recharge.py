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
from LocalDatabase import get_bills_connection, get_cash_billing_connection
from produce_month_index import MonthIndexFactroy

class UserRecharge:

  bill_100_students = set()
  bill_students = set()

  def __init__(self, date):
    self.conn = get_bills_connection()
    self.cash_bill_conn = get_cash_billing_connection()
    self.cur = self.conn.cursor()
    self.output_file = 'data/user_recharge.csv'

    self.small_product_id = (4,8,10, 11, 12, 20, 21, 22, 23, 24, 25, 26, 28, 27, 29, 
    1003, 1004, 1005, 1006, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 1031, 1032,
    1033, 1034, 1035, 1036, 1037, 1038)
    self.big_product_id = (5, 6, 13, 19)
    self.all_product_id = self.big_product_id + self.small_product_id

    self.month_index = MonthIndexFactroy()

  def query_month(self, m):
    print(m)
    sql = "select id, product_id, student_id, actual_price \
      from bills where  paid_at between %r and %r and product_id \
      in %s and deleted_at is null and status in (20, 80) " % (m.begin, m.end, str(self.all_product_id))
    self.cur.execute(sql)
    data = self.cur.fetchall()

    if m.name >= '2018-03':
      with self.cash_bill_conn.cursor() as cur:
        sql = "select id, case product_id when 1 then 4 else 5 end, student_id, \
        actual_price from bills where paid_at between %r and %r  and product_type \
        = 0 and product_id in (1,2,3,4) and status in (20, 80) and deleted_at is null" % (m.begin, m.end)
        cur.execute(sql)
        data += cur.fetchall()

    s1 = set()
    s2 = set()
    actual_price = float()

    for row in data:
      if row[1] in self.small_product_id:
        s1.add(row[2])
      else:
        if not row[2] in self.bill_students:
          s2.add(row[2])
          actual_price += float(row[3])
          self.bill_students.add(row[2])

    return s1, s2, actual_price

  def combin(self):
    output = list()
    for m in self.month_index.output:
      s1, s2, aprice = self.query_month(m)
      self.bill_100_students.update(s1)
      total_100 = len(self.bill_100_students)
      new_100 = len(s1)
      
      new_bill = len(s2)
      bill = len(self.bill_students)
      print('%d, %d, %d, %d, %f' % (total_100, new_100, bill, new_bill, aprice))
      output.append((m.name, total_100, new_100, 100 * new_100, bill, new_bill, aprice))
    self.save(output)

  def save(self, data):
    with open(self.output_file, 'w', newline='') as csvfile:
      spamwriter = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_MINIMAL) 
      spamwriter.writerow(['month','累计体验', '新体验用户', '体验流水', '累计长期用户', '新长期用户',  '长期用户流水' ])
      for row in data:
        spamwriter.writerow(row)

if __name__ == "__main__":
  u = UserRecharge('2018-05')
  u.combin()
  #print(u.get_month_range())
      
