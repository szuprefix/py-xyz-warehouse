# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from xyz_etl.helper import get_intervals
from . import tasks, models
import logging

log = logging.getLogger("django")


def start_flow_job(now=None):
    ints = get_intervals(now)
    log.info("start flow job in intervals: %s" % ints)
    from xyz_etl import helper
    helper.check_system_expire()
    for f in models.Flow.objects.filter(interval__in=ints, is_active=True):
        if f.is_running:
            log.warning("flow %s old task is still running where new task start. interval :%s " % (f, f.interval))
            # continue
        f.run_async()


def check_all_async_log_state():
    from xyz_util.dateutils import get_next_date
    yesterday = get_next_date(days=-1)
    es = []
    for alog in models.AsyncLog.objects.filter(create_time__gt=yesterday, total_end_time__isnull=True):
        try:
            alog.check_state()
        except Exception, e:
            es.append("%s %s" % (alog, unicode(e)))
    if len(es) > 0:
        log.error("check_all_async_log_state errors: %s", "\n".join(es))


def clear_old_async_log():
    from xyz_util.dateutils import get_next_date
    old_day = get_next_date(days=-7)
    models.AsyncLog.objects.filter(create_time__lt=old_day).delete()


def data_compare_cron(now=None):  # 定时核对数据任务
    from . import models
    ints = get_intervals(now)
    log.info("start datacompare job in intervals: %s" % ints)
    for dc in models.DataCompare.objects.filter(interval__in=ints):
        log.info("try to start datacompare task: %s", dc.id)
        rs = tasks.datacompare.apply_async([dc.id])  # 异步任务请求


def ods_table_run_stat():
    from xyz_util import dateutils
    from . import stat
    yesterday = dateutils.get_next_date(days=-1)
    for t in models.ODSTable.objects.all():
        print t
        try:
            if t.has_delete_action:
                c = t.delete_disappear_records()
                print "delete", c
            t.compare_and_sync(begin_time=yesterday)
            t.run_stat()
            stat.create_daily_result(t)
        except Exception, e:
            import traceback
            log.warning("ods_table_run_stat %s error: %s ", t, traceback.format_exc())


