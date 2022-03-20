# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

CALTYPE_EQUATION = 1
CALTYPE_MAP = 2

CHOICIES_CALTYPE = ((CALTYPE_EQUATION, "公式"), (CALTYPE_MAP, "映射表"))

INTERVAL_NONE = '--'
CHOICES_INTERVAL = (
    (INTERVAL_NONE, "不定时"),
    ('2m', "2分钟"),
    ('5m', "5分钟"),
    ('10m', "10分钟"),
    ('20m', "20分钟"),
    ('1h', "1小时"),
    ('2h', "2小时"),
    ('4h', "4小时"),
    ('1d@h1', "每天1点"),
    ('1d@h2', "每天2点"),
    ('1d@h4', "每天4点"),
    ('1d@h6', "每天6点"),
    ('1d@h19', "每天19点"),
    ('1d@h21', "每天21点")
)

WRITE_MODE_DELTA_ASYNC = 1
WRITE_MODE_DELTA_SYNC = 2
WRITE_MODE_REPLACE = 4

CHOICES_WRITE_MODE = (
    # (WRITE_MODE_DELTA_ASYNC, "异步增量"),
    (WRITE_MODE_DELTA_SYNC, "同步增量"),
    (WRITE_MODE_REPLACE, "全量替换")
)

ASYNCLOG_STATUS_PENDING = 1
ASYNCLOG_STATUS_RUNNING = 2
ASYNCLOG_STATUS_ERROR = 4
ASYNCLOG_STATUS_SUCCESS = 8

CHOICES_ASYNCLOG_STATUS = (
    (ASYNCLOG_STATUS_PENDING, "排队中"),
    (ASYNCLOG_STATUS_RUNNING, "执行中"),
    (ASYNCLOG_STATUS_ERROR, "出错"),
    (ASYNCLOG_STATUS_SUCCESS, "成功"),
)

FLOW_MODE_TIME = 1
FLOW_MODE_PK = 2
FLOW_MODE_CURSOR = 4

CHOICES_FLOW_MODE = (
    (FLOW_MODE_CURSOR, "游标模式"),
    (FLOW_MODE_TIME, "时间模式"),
    # (FLOW_MODE_PK, "主键模式")
)

ON_DUPLICATE_DO_REPLACE = 'replace'
ON_DUPLICATE_DO_IGNORE = 'ignore'
CHOICES_ON_DUPLICATE_DO = (
    (ON_DUPLICATE_DO_REPLACE, "替换更新"),
    (ON_DUPLICATE_DO_IGNORE, "忽略跳过")
)
