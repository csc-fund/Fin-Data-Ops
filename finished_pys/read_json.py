#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :read_json.py
# @Time      :2022/9/15 09:35
# @Author    :Colin
# @Note      :None
import pandas as pd
import json

with open('map_dict.json') as f:
    data = json.load(f)
