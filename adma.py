#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: adma.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: COO 所需数据
# @Create: 2018-11-25 19:00:56
# @Last Modified: 2018-11-25 19:00:56
#
from LocalDatabase import get_schedule_connection, get_product_connection
import pandas as pd
import pdb

class M1:

    def get_users(self):
        conn = get_product_connection()
        sql = "select user_id, first_large_buy_at  from users  where vip = 1 and first_large_buy_at > '2016-01-01' and deleted_at is null"
        df = pd.read_sql(sql, conn, index_col = 'user_id')
        df['count'] = 0
        self._df = df

    def count_consume(self):
        conn = get_schedule_connection()
        sql = "select student_id, created_at from student_appointments \
                    where status = 3 and deleted_at is null and created_at > \
                    '2016-01-01'"
        with conn.cursor() as cur:
            cur.execute(sql)
            while cur.rownumber < cur.rowcount:
                
                student, date = cur.fetchone()
                if student in self._df.index:
                    a = date - self._df.loc[student, 'first_large_buy_at'] 
                    if a.days < 365:
                        self._df.loc[student,'count'] += 1
                if cur.rownumber % 10000 == 0:
                    print(date)

        self._df.to_csv('user_comsume.csv')

if __name__ == "__main__":
    m = M1()
    m.get_users()
    m.count_consume()

