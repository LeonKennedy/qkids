#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: produce_month_index.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: ---
# @Create: 2018-05-29 11:40:39
# @Last Modified: 2018-05-29 11:40:39
#

from datetime import datetime
import pdb


class MonthIndex:
  
  def __init__(self, begin, end):
    self.begin = begin
    self.end = end
    self.name = begin[0:7]

    
  def __str__(self):
    return '(%r, %r)' % (self.begin, self.end)
    

class MonthIndexFactroy:
  def __init__(self, end = None, begin = '2015-12'):
    now = datetime.now()
    self.begin = begin
    self.end = end if end else '%d-%02d' % (now.year, now.month)
    self.output = self.product_index()
    self.len = len(self.output)
    self.index = [ i.name for i in self.output ]

  def product_index(self):
    output = list()
    output.append(MonthIndex('2015-12-01','2016-01-01'))
    output.extend([ MonthIndex('2016-%02d-01' % m, '2016-%02d-01' % (m+1)) for m in range(1,12) ])
    output.append(MonthIndex('2016-12-01', '2017-01-01'))
    output.extend([ MonthIndex('2017-%02d-01' % m, '2017-%02d-01' % (m+1)) for m in range(1,12) ])
    output.append(MonthIndex('2017-12-01', '2018-01-01'))
    output.extend([ MonthIndex('2018-%02d-01' % m, '2018-%02d-01' % (m+1)) for m in range(1,12) ])
    return [ m for m in output if m.name <= self.end and m.name >= self.begin]

if __name__ == "__main__":
  m = MonthIndexFactroy(begin = '2017-11')
  for i in m.product_index():
    print(i)
    
