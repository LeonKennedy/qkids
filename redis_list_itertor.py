#!/usr/bin/env python
#-*- coding: utf-8 -*-
# @Filename: redis_list_itertor.py
# @Author: olenji - lionhe0119@hotmail.com
# @Description: ---
# @Create: 2018-11-05 13:40:20
# @Last Modified: 2018-11-05 13:40:20
#


import pdb
from QkidsRedis import get_schedule_redis

def search_list(key, words):
  r = get_schedule_redis()
  items = True
  start = 0
  end = 1000
  while items:
    items = r.lrange(key, start, end)
    for item in items:
      for word in words:
        if item.rfind(word) >= 0:
          print(item)
    start, end = end, end+1000
    print(start)


if __name__ == "__main__":
  search_list('TEACHER_ARRANGEMENT', [b'3156573', b'114646'])

