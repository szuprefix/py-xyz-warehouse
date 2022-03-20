# -*- coding:utf-8 -*-
__author__ = 'denishuang'
from celery import shared_task
from celery.result import AsyncResult
from . import models
from django.http.request import QueryDict
from celery.utils.log import get_task_logger
import urllib2, json
from django.conf import settings
from logging import getLogger
from datetime import datetime

log = getLogger("celery")  # get_task_logger(__name__)

NOTIFY_BASE_URL = settings.NOTIFY_BASE_URL


def update_task_end_log(request, begin_time, affect_records=0, errors=[]):
    try:
        rid = request.get("root_id")
        errors_count = errors and len(errors) or 0
        task = dict(root_id=rid,
                    id=request.get("id"),
                    cost_seconds=(datetime.now() - begin_time).seconds,
                    affect_records=affect_records,
                    end_time=datetime.now(),
                    errors_count=errors_count,
                    success_count=affect_records - errors_count)
        alog = models.AsyncLog.objects.filter(task_id=rid).first()
        if alog:
            alog.update_task_state(task)
    except Exception:
        import traceback
        log.error("update_task_end_log error: %s", traceback.format_exc())


def update_task_begin_log(request):
    try:
        rid = request.get("root_id")
        task = dict(
            root_id=rid,
            id=request.get("id"),
            begin_time=datetime.now()
        )
        models.AsyncLog.objects.get(task_id=rid).update_task_state(task)
    except Exception:
        import traceback
        log.error("update_task_begin_log error: %s", traceback.format_exc())


@shared_task(bind=True, time_limit=72000)
def flow(self, id, context={}, update_break_point=True):
    begin_time = datetime.now()
    try:
        task_id = self.request.get("id")
        flow = models.Flow.objects.get(id=id)
        flow.async_logs.filter(task_id=task_id).update(begin_time=datetime.now())
        update_task_begin_log(self.request)
        if not context:
            context = flow.get_default_context()
        log.info("start task %s context:%s", flow, context)
        setattr(self, 'affect_records', 0)

        def update_progress(result):
            if isinstance(result, dict):
                d = result
            else:
                d = dict(result=str(result))

            self.update_state(d.get('state', 'STARTED'), meta=d)
            affect_records = d.get('affect_records')
            if affect_records:
                setattr(self, 'affect_records', affect_records)

        flow.run(context, progress=update_progress)
        if update_break_point:
            models.Flow.objects.filter(id=id).update(break_point=context.get('end_time'))
        models.AsyncLog.objects.filter(task_id=task_id).update(end_time=datetime.now())
        update_task_end_log(self.request, begin_time, self.affect_records)
        return "success"
    except Exception, e:
        import traceback
        errmsg = "warehouse.tasks.flow exception:%s" % traceback.format_exc()
        log.error(errmsg)
        models.AsyncLog.objects.filter(task_id=task_id).update(end_time=datetime.now())
        update_task_end_log(self.request, begin_time)
        raise e


@shared_task(bind=True, time_limit=1800)
def auto_growup_table_insert(self, args, json_str):
    import pandas as pd
    from xyz_util.pandasutils import AutoGrowTable
    begin_time = datetime.now()
    update_task_begin_log(self.request)
    table = AutoGrowTable(*args)
    df = pd.read_json(json_str, dtype=False)
    errors = table.run(df)
    count = len(df)
    if errors:
        log.error("task auto_growup_table_insert %s done: count=%d ; errors=%s", args, count, errors)
    else:
        log.info("task auto_growup_table_insert %s done: count=%d ;", args, count)
    update_task_end_log(self.request, begin_time, count, errors=errors)
    return "success"


@shared_task(bind=True, time_limit=60)
def notify(self, data, url):
    update_task_begin_log(self.request)
    begin_time = datetime.now()
    if not url.startswith("http"):
        url = "%s%s" % (NOTIFY_BASE_URL, url)
    qd = QueryDict(mutable=True)
    qd.update(data)
    url = "%s?%s" % (url, qd.urlencode())
    headers = {"Content-Type": "application/json", "Accept": "text/plain"}
    # data and json.dumps(data, ensure_ascii=False).encode("utf8")
    log.info(url)
    request = urllib2.Request(url, "{}", headers=headers)
    s = urllib2.urlopen(request).read()
    update_task_end_log(self.request, begin_time, 1)
    return "success"


@shared_task(time_limit=60)
def just_test_log(info):
    log.error(info)


# @shared_task()
# def check_async_log_state(task):
#     log.info("check_async_log_state start:%s", task.get("root_id"))
#     alog = models.AsyncLog.objects.get(task_id=task.get("root_id"))
#     alog.check_state(task)
#     log.info("check_async_log_state end:%s", task.get("root_id"))


###xia 增加数据对比的任务
from .helper import mail_data_to_datacompare


@shared_task(bind=True, time_limit=3600)
def datacompare(self, id, context={}):
    begin_time = datetime.now()
    log.info("run datacompare task: %s", id)
    try:
        task_id = self.request.get("id")
        datacompare = models.DataCompare.objects.get(id=id)
        if not context:
            context = datacompare.get_default_context()
        log.info("start task %s context:%s", datacompare, context)
        datacompare.run(context, progress=lambda x: None)
        models.DataCompare.objects.filter(id=id).update(break_point=context['end_time'])
        return "success"
    except Exception, e:
        errmsg = "warehouse.tasks.datacompare exception:%s" % e
        log.error(errmsg)
