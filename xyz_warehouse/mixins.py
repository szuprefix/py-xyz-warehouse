# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals


class IORunable(object):
    def run(self, data, context={}, progress=lambda x: None):
        return data
