# -*- coding:utf-8 -*-
from __future__ import unicode_literals, print_function

from django.utils.functional import cached_property

from . import choices, mixins
from django.db import models
from xyz_util.modelutils import CodeMixin, JSONField
import logging
from datetime import datetime
from collections import OrderedDict

OUTPUT_BATCH_SIZE = 2000

ASYNC_TASK_QUEUE_CONTEXT = 'async_task_queue'
OUTPUT_WRITE_MODE_CONTEXT = 'output_write_mode'
FLOW_TASK_LINES_CONTEXT = 'flow_task_lines'

log = logging.getLogger("django")


class Project(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '项目'

    name = models.CharField('名称', max_length=64)
    update_timestamp_field = models.CharField('update时间戳字段名', max_length=64, null=True, blank=True,
                                              help_text="如果设置了该字段,则该工程下所有输出源均会自动维护此字段")
    insert_timestamp_field = models.CharField('insert时间戳字段名', max_length=64, null=True, blank=True,
                                              help_text="如果设置了该字段,则该工程下所有输出源均会自动维护此字段")

    def __str__(self):
        return self.name

    def save(self, **kwargs):
        if self.update_timestamp_field:
            self.update_timestamp_field = self.update_timestamp_field.strip()
        if self.insert_timestamp_field:
            self.insert_timestamp_field = self.insert_timestamp_field.strip()
        return super(Project, self).save(**kwargs)


class Rule(CodeMixin, mixins.IORunable, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '计算规则'

    name = models.CharField('名称', max_length=64)
    code = models.CharField("代号", max_length=64, blank=True, default="")
    description = models.TextField('描述', blank=True, null=True)
    content = models.TextField("公式/映射表",
                               help_text="每行一条工式, 例子:\n变量1=变量2*Rank()\n\n\n条件表达式: \nCondition([0,1)=100;[1,3)=200;[3,inf]=300)\n排名表达式:\nRank()\nRank(%)\n")
    is_active = models.BooleanField('有效', default=True, blank=True)
    outputs = models.TextField("输出", blank=True, default="",
                               help_text="计算类型为公式的输出字段可以有多个,使用空格或者换行分开. \n计算类别为映射表的只能有一个输出字段")
    caltype = models.PositiveSmallIntegerField('计算类别', blank=True, choices=choices.CHOICIES_CALTYPE,
                                               default=choices.CALTYPE_EQUATION)
    create_time = models.DateTimeField("创建时间", auto_now_add=True)
    test_case = models.TextField("测试用例", blank=True, default="", help_text="一行一个, 例子: \n保有量:23 技能排名:0.2 => 等级排名:3")
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)

    def save(self, **kwargs):
        if not self.outputs:
            self.outputs = "\n".join(self.calculate_object.outputs)
        return super(Rule, self).save(**kwargs)

    def __str__(self):
        return "%s(%s)" % (self.name, self.code)

    @property
    def calculate_object(self):
        if not hasattr(self, "_calculate_object"):
            from .helper import Rule, Map
            self._calculate_object = self.caltype == choices.CALTYPE_MAP and Map(self.content, self.outputs) or Rule(
                self.content)

        return self._calculate_object

    @property
    def vars(self):
        from helper import RE_SPACES
        if not self.outputs:
            return {}
        outputs = set(RE_SPACES.split(self.outputs))
        try:
            co = self.calculate_object
            temps = set(co.outputs).difference(outputs)
            inputs = set(co.inputs)
        except:
            temps = ()
            inputs = ()
        return {"outputs": outputs, "temps": temps, "inputs": inputs}

    def run(self, data, context={}, progress=lambda x: None):
        progress("计算规则%s正在运行中" % self)
        return self.calculate_object.run(data)

    def run_test(self):
        import pandas as pd
        from xyz_util.datautils import split_test_case
        from .helper import RE_SPACES
        rs, cs = split_test_case(self.test_case)
        df = pd.DataFrame(rs).fillna(0)
        rs = self.run(df)
        outputs = RE_SPACES.split(self.outputs)
        return {"run_result": rs.to_dict('records'), "case_result": cs, "outputs": outputs}


class Map(CodeMixin, mixins.IORunable, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '映射表'

    name = models.CharField('名称', max_length=64)
    code = models.CharField("代号", max_length=64, blank=True, default="")
    content = models.TextField("规则",
                               help_text="首行为表头, 之后每行为一条记录, 字段间用空格拆分.例子:\n测试题1 测试题2 测试题3 测试归类\n13 24 31 2\n13 23 32 3")
    is_active = models.BooleanField('有效', default=True, blank=True)
    input_fields = models.TextField("输入字段", blank=True, default="")
    output_field = models.CharField("输出字段", max_length=64, blank=True, default="")
    create_time = models.DateTimeField("创建时间", auto_now_add=True)
    test_case = models.TextField("测试用例", blank=True, default="",
                                 help_text="一行一个, 例子: \n测试题1:13 测试题2:24 测试题3:31 => 测试归类:2")

    @property
    def calculate_object(self):
        if not hasattr(self, "_calculate_object"):
            from .helper import Map
            self._calculate_object = Map(self.content)

        return self._calculate_object

    def run(self, context):
        return self.calculate_object.run(context)

    def save(self, **kwargs):
        self.content = self.content.strip()
        co = self.calculate_object
        self.output_field = co.output_field
        # print co.input_fields
        self.input_fields = ",".join(co.input_fields)
        return super(Map, self).save(**kwargs)

    def run_test(self):
        import pandas as pd
        from xyz_util.datautils import str2dict
        from xyz_util.datautils import split_test_case
        rs, cs = split_test_case(self.test_case)
        df = pd.DataFrame(rs)
        rs = self.run(df)
        outputs = str2dict(self.output_field, line_spliter=" ").keys()
        return {"run_result": rs.to_dict('records'), "case_result": cs, "outputs": outputs}

    def __str__(self):
        return "%s(%s)" % (self.name, self.code)


class Input(CodeMixin, mixins.IORunable, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '输入源'

    name = models.CharField('名称', max_length=64)
    code = models.CharField('代号', max_length=64, blank=True, default="", unique=True)
    db = models.ForeignKey('etl.source', verbose_name="源库")
    table_name = models.TextField('SQL', null=True, blank=True, help_text='支持只填表名')
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)
    end_time_field = models.CharField('结束时间字段', max_length=128, blank=True, null=True,
                                      help_text="用来自动检测断点endtime, 若留空则使用datetime.now(), 格式: 表名.字段名")

    # def save(self, **kwargs):
    #     return super(Input, self).save(**kwargs)

    def get_max_timestamp(self):
        if not self.end_time_field:
            return datetime.now()
        ps = self.end_time_field.split(".")
        fname = ps[-1]
        tname = ".".join(ps[:-1])
        sql = "select max(%s) as mt, 1 as none from %s" % (fname, tname)
        #  '1 as none' 只为占位, 以防postgresql timestamp with time zone类型的时间字段导致下面的 .loc[0][0]出现下标错误
        from xyz_util import pandasutils
        df = pandasutils.tz_convert(self.db.query(sql))
        d = df.loc[0][0] or datetime.now()
        return d

    @property
    def data(self):
        if not hasattr(self, '_data'):
            from xyz_util import dateutils
            begin_time = dateutils.get_next_date(None, -1).isoformat()
            end_time = dateutils.format_the_date().isoformat()
            context = dict(begin_time=begin_time, end_time=end_time)
            self._data = self.run({}, context)
        return self._data

    def run(self, data, context={}, progress=lambda x: None):
        from django.template import Template, Context
        template = Template(self.table_name)
        if not context.get('end_time'):
            context['end_time'] = self.get_max_timestamp().isoformat()
        pks = context.get("batch_primary_keys")
        if pks:
            sqls = []
            for pk in pks:
                ctx = {}
                ctx.update(pk)
                ctx.update(context)
                sqls.append(template.render(Context(ctx)))
            sql = " \n union all \n".join(sqls)
        else:
            sql = template.render(Context(context))
        log.info(sql)
        progress("输入源%s读取数据中, sql: %s" % (self, sql))
        return self.db.query(sql, coerce_float=False)

    def __str__(self):
        return self.name == self.code and self.name or "%s(%s)" % (self.name, self.code)


class Output(CodeMixin, mixins.IORunable, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '输出源'

    name = models.CharField('名称', max_length=64)
    code = models.CharField('代号', max_length=64, blank=True, default="", unique=True)
    db = models.ForeignKey('etl.source', verbose_name="数据库")
    table_name = models.CharField('表名', max_length=256)
    primary_key = models.CharField('主键', max_length=64, help_text="如果是多字段复合主键请用逗号','分隔")
    write_mode = models.PositiveSmallIntegerField('更新模式', choices=choices.CHOICES_WRITE_MODE,
                                                  default=choices.WRITE_MODE_DELTA_ASYNC,
                                                  help_text="同步增量:单进程串行插入,全量替换:先truncate再插入")
    encrypt_fields = models.TextField('加密字段', null=True, blank=True, help_text="一行一个")
    extra_updates = models.TextField('额外更新', null=True, blank=True, default='',
                                     help_text="在数据插入完毕后执行,多条语句用';'号加换行分隔, 注:异步增量模式不支持此功能")
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)

    def to_encrypt_fields(self, df):
        s = self.encrypt_fields and self.encrypt_fields.strip()
        if not s:
            return df
        from xyz_util.cryptutils import des_encrypt
        from xyz_util.datautils import space_split
        log.info("Output  %s encrypt_fields: %s" % (self, space_split(self.encrypt_fields)))
        for f in space_split(self.encrypt_fields):
            if f in df:
                df[f] = df[f].apply(des_encrypt)
        return df

    def save(self, **kwargs):
        self.extra_updates = self.extra_updates.strip()
        return super(Output, self).save(**kwargs)

    def run(self, data, context={}, progress=lambda x: None):
        data = self.to_encrypt_fields(data)
        from xyz_util import pandasutils
        log.info("Output %s count:%s" % (self, data.shape))
        write_mode = context.get(OUTPUT_WRITE_MODE_CONTEXT, self.write_mode)
        log.info("write_mode %s " % (write_mode))
        if write_mode == choices.WRITE_MODE_REPLACE:
            progress("输出源%s以全量替换方式输出数据到表%s" % (self, self.table_name))
            tps = self.table_name.split(".")
            table_name = tps[-1]
            schema = len(tps) > 1 and tps[0] or None
            data.to_sql(table_name, self.db.alchemy_str, if_exists="replace", index=False, schema=schema, chunksize=100)
        else:
            table = pandasutils.AutoGrowTable(self.db.connection, self.table_name, self.primary_key,
                                              self.project and self.project.insert_timestamp_field,
                                              self.project and self.project.update_timestamp_field)
            table.create_table(data)
            if write_mode == choices.WRITE_MODE_DELTA_ASYNC:
                from . import tasks
                # data = pandasutils.format_timestamp(data)
                progress("输出源%s以异步增量方式输出数据到表%s" % (self, self.table_name))
                queue = context.get(ASYNC_TASK_QUEUE_CONTEXT)
                for dfc in pandasutils.split_dataframe_into_chunks(data, OUTPUT_BATCH_SIZE):
                    dfc = pandasutils.tz_convert(dfc)
                    rs = tasks.auto_growup_table_insert.apply_async([[self.db.name, self.table_name, self.primary_key,
                                                                      self.project and self.project.insert_timestamp_field,
                                                                      self.project and self.project.update_timestamp_field],
                                                                     dfc.to_json(date_format='iso')], queue=queue)
                    log.info("start tasks auto_growup_table_insert %s for %s", rs, self)
            else:
                progress("输出源%s以同步增量方式输出数据到表%s" % (self, self.table_name))
                errors = table.run(data)
                if errors:
                    log.error("Output %s run got error: %s", self, errors)
                progress(dict(affect_records=len(data)))
                self.run_extra_updates(context)

        return data

    def run_extra_updates(self, context):
        if not self.extra_updates:
            return
        from django.template import Template, Context
        sql = Template(self.extra_updates).render(Context(context))
        import re
        r = re.compile(r";\s*")
        for s in r.split(sql):
            if s.strip():
                log.info(s)
                self.db.execute(s)

    def __str__(self):
        return self.name == self.code and self.name or "%s(%s)" % (self.name, self.code)


class Notify(CodeMixin, mixins.IORunable, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '回调'

    name = models.CharField('名称', max_length=64)
    code = models.CharField('代号', max_length=64, blank=True, default="", unique=True)
    api = models.CharField('接口地址', max_length=128)
    content = models.TextField("内容", help_text="字典映射", blank=True, default="")
    is_active = models.BooleanField('有效', default=True, blank=True)
    create_time = models.DateTimeField("创建时间", auto_now_add=True)

    @property
    def default_data(self):
        from xyz_util.datautils import str2dict
        return str2dict(self.content, " ")

    def run(self, data, context={}, progress=lambda x: None):
        default_data = self.default_data
        c = 0
        queue = context.get(ASYNC_TASK_QUEUE_CONTEXT)  # context.get("high_priority") and "high" or None
        for k in data.index:
            d = {}
            d.update(default_data)
            d.update(data.loc[k].to_dict())
            from .tasks import notify
            log.info("start task notify: %s", d)
            notify.apply_async([d, self.api], queue=queue)
            c += 1
            if c % 100 == 0:
                progress("%s:%d" % (self, c))
        log.info("Notify %s count:%s" % (self, data.shape))
        return data

    def __str__(self):
        return "%s(%s)" % (self.name, self.code)


class SelectExclude(CodeMixin, mixins.IORunable, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '查询过滤'

    name = models.CharField('名称', max_length=64)
    code = models.CharField('代号', max_length=64, blank=True, default="", unique=True)
    db = models.ForeignKey('etl.source', verbose_name="源库")
    sql = models.TextField('SQL', help_text="传参格式例如: {{user_id}}")
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)

    def __str__(self):
        return "%s(%s)" % (self.name, self.code)

    def run(self, data, context={}, progress=lambda x: None):
        from django.template import Context, Template
        st = Template(self.sql)
        ids = []
        for i in data.index:
            d = data.loc[i].to_dict()
            sql = st.render(Context(d))
            df = self.db.query(sql)
            if len(df) == 0 or df.loc[0][0] == 0:
                ids.append(i)
                print(d)
        return data.loc[ids]


class Flow(CodeMixin, models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '数据流'

    name = models.CharField('名称', max_length=64)
    code = models.CharField('代号', max_length=64, blank=True, default="", unique=True)
    description = models.TextField('描述', blank=True, null=True)
    content = models.TextField("内容", help_text="用|把各个节点串联起来.例如: Input(code:XDDF)|Rule(id:2)|Rule(id:9)|Output(id:10)")
    is_active = models.BooleanField('有效', default=True, blank=True)
    create_time = models.DateTimeField("创建时间", auto_now_add=True)
    break_point = models.DateTimeField("断点时间", blank=True, default=datetime.now)
    interval = models.CharField("定时任务", max_length=8, choices=choices.CHOICES_INTERVAL, default=choices.INTERVAL_NONE)
    # relay_status_source = models.ForeignKey("etl.source", verbose_name="备库源", null=True, blank=True,
    #                                         help_text="从刚数据源读取最后同步时间作为正式的断点, 避免因数据库同步延迟导致错过相关增量数据.")
    # high_priority = models.BooleanField("高优先级", default=False, blank=True)
    queue = models.ForeignKey("asynctask.Queue", verbose_name="通道", null=True, blank=True,
                              related_name="warehouse_flows")
    result_code = models.CharField("异步任务编码", max_length=128, blank=True, null=True)
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)

    def __str__(self):
        return self.name == self.code and self.name or "%s(%s)" % (self.name, self.code)

    def get_new_break_point(self):
        relay_status_source = self.baseDB
        if relay_status_source and relay_status_source.is_slave:
            db = relay_status_source
            from xyz_util import dbutils
            end_time = dbutils.get_slave_time(db.connection)

            if not end_time:
                raise Exception("%s get_new_break_point at %s got end_time is None Exception." % (self, db))
            return end_time.isoformat()
        else:
            from datetime import datetime
            return datetime.now().isoformat()

    def get_default_context(self):
        from xyz_util.dateutils import format_the_date
        begin_time = self.break_point and self.break_point.isoformat() or format_the_date().isoformat()
        # end_time = self.get_new_break_point()
        return {"begin_time": begin_time}  # , "end_time": end_time

    @property
    def async_result(self):
        if not self.result_code:
            return None
        from celery.result import AsyncResult
        return AsyncResult(self.result_code)

    @property
    def queue_code(self):
        return self.queue and self.queue.code or None

    @property
    def iterdeps(self):
        rs = self.async_result
        ds = []
        if not rs:
            return []
        for i in rs.iterdeps(intermediate=True):
            crs = i[1]
            ds.append(dict(id=crs.id, state=crs.state, result=crs.result))
        return ds

    def save_log(self, task_id):
        self.result_code = task_id
        AsyncLog.objects.create(task_id=task_id, flow=self)

    @property
    def tasks(self):
        return self.gen_tasks(self.content)

    def gen_tasks(self, s):
        from . import nodes
        ts = [
            [nodes.check_node(a) for a in l.split("|")]
            for l in s.split("\n")
            if l.strip() and not l.strip().startswith("--")
        ]
        return ts

    @property
    def baseDB(self):
        try:
            return self.tasks[0][0].get_object().db
        except Exception, e:
            return

    def is_still_running(self):
        last_run = self.async_result
        return last_run and not (last_run.failed() or last_run.successful())

    def run(self, context={}, progress=lambda x: None):
        progress("START")
        if not context:
            context = self.get_default_context()

        context.setdefault(ASYNC_TASK_QUEUE_CONTEXT, self.queue_code)

        task_lines = context.get(FLOW_TASK_LINES_CONTEXT, self.content)
        for t in self.gen_tasks(task_lines):
            progress("%s" % t)
            d = context
            for n in t:
                progress("%s" % n)
                d = n.run(d, context, progress)
                # rs.append(d)
        return context

    # def run_again(self, context):
    #     if not self.interval or self.interval == choices.INTERVAL_NONE:
    #         return
    #     begin_time = context.get("begin_time")
    #     end_time = context.get("end_time")

    def test(self, context={}):
        rs = []
        for t in self.tasks:
            d = {}
            for n in t:
                d = n.run(d, context)
            rs.append(d)
        return rs

    def terminate_all(self):
        res = []
        from django.forms.models import model_to_dict
        for al in self.async_logs.filter(status__in=(choices.ASYNCLOG_STATUS_PENDING, choices.ASYNCLOG_STATUS_RUNNING)):
            al.terminate_all()
            res.append(model_to_dict(al, fields=["task_id", "create_time"]))
        return res

    def run_async(self, context={}, update_break_point=True):
        from . import tasks
        queue = context.get(ASYNC_TASK_QUEUE_CONTEXT, self.queue_code)
        log.info("start flow %s(%s)", self, context)
        rs = tasks.flow.apply_async(
            [self.id],
            kwargs=dict(context=context, update_break_point=update_break_point),
            queue=queue
        )
        self.async_logs.update_or_create(task_id=rs.task_id, defaults=dict(context=context))
        self.result_code = rs.id
        self.save()
        return rs

    @property
    def is_running(self):
        if self.result_code:
            alog = AsyncLog.objects.filter(task_id=self.result_code).first()
            if alog and not alog.finished:
                return True
        return False


class DataCompare(CodeMixin, models.Model):  # xia 20180820 增加 数据对比监控
    class Meta:
        verbose_name_plural = verbose_name = '数据对比'

    name = models.CharField('名称', max_length=64)
    code = models.CharField('代号', max_length=64, blank=True, default="", unique=True)
    description = models.TextField('描述', blank=True, null=True)
    db1 = models.ForeignKey('etl.source', verbose_name="库源", related_name='warehouse_datacompares_db1')
    db2 = models.ForeignKey('etl.source', verbose_name="库源", related_name='warehouse_datacompares_db2')
    sql1 = models.TextField("sql1", max_length=300, blank=True)
    sql2 = models.TextField("sql2", max_length=300, blank=True)
    is_active = models.BooleanField('有效', default=True, blank=True)
    create_time = models.DateTimeField("创建时间", auto_now_add=True)
    break_point = models.DateTimeField("断点时间", blank=True, default=datetime.now)
    interval = models.CharField("定时任务", max_length=8, choices=choices.CHOICES_INTERVAL, default=choices.INTERVAL_NONE)
    queue = models.ForeignKey("asynctask.Queue", verbose_name="通道", null=True, blank=True,
                              related_name="warehouse_datacompare")
    result_code = models.CharField("异步任务编码", max_length=128, blank=True, null=True)
    project = models.ForeignKey('warehouse.Project', verbose_name='项目', null=True, blank=True)

    def __str__(self):
        return "%s(%s)" % (self.name, self.code)

    def get_new_break_point(self):
        from datetime import datetime
        return datetime.now().isoformat()

    def get_default_context(self):
        from xyz_util.dateutils import format_the_date
        begin_time = self.break_point and self.break_point.isoformat() or format_the_date()
        end_time = self.get_new_break_point()
        return {"begin_time": begin_time, "end_time": end_time}

    def run_sql(self, db, sql, progress=lambda x: None):
        log.info(sql)
        progress("数据对比%s读取数据中, sql: %s" % (self, sql))
        return db.query(sql)

    def run(self, context={}, progress=lambda x: None):
        from .helper import mail_data_to_datacompare
        from django.template import Template, Context
        sql1 = Template(self.sql1).render(Context(context))
        sql2 = Template(self.sql2).render(Context(context))
        data1 = self.run_sql(self.db1, sql1)
        data2 = self.run_sql(self.db2, sql2)
        data2 = data2.append(data1)
        data2 = data2.append(data1)
        result = data2.drop_duplicates(subset=['name'], keep=False)  # 将两个dataframe做差集

        dict_result = {'dataframe': result, 'filename': '%s.xlsx' % self.name}
        from django.conf import settings
        mail_data_to_datacompare(dict_result,
                                 "%s%s--%s数据对比邮件" % (self.name, context.get("begin_time"), context.get("end_time")), "",
                                 [a[1] for a in settings.ADMINS])

    @property
    def queue_code(self):
        return self.queue and self.queue.code or None


class AsyncLog(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = '异步日志'
        ordering = ('-create_time',)

    task_id = models.CharField("任务ID", max_length=128, unique=True)
    flow = models.ForeignKey(Flow, verbose_name=Flow._meta.verbose_name, related_name="async_logs")
    create_time = models.DateTimeField("创建时间", auto_now_add=True, db_index=True)
    begin_time = models.DateTimeField("开始时间", null=True, blank=True)
    end_time = models.DateTimeField("结束时间", null=True, blank=True)
    total_end_time = models.DateTimeField("完全结束时间", null=True, blank=True)
    cost_seconds = models.PositiveIntegerField("耗时秒数", blank=True, null=True)
    affect_records = models.PositiveIntegerField("影响数据量", blank=True, null=True)
    status = models.PositiveSmallIntegerField("状态", blank=True, null=True, db_index=True,
                                              choices=choices.CHOICES_ASYNCLOG_STATUS)
    children = JSONField("子任务列表", blank=True, null=True)
    context = JSONField("参数", blank=True, null=True)
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)

    def save(self, **kwargs):
        if self.project is None:
            self.project = self.flow.project
        if self.status is None:
            self.status = choices.ASYNCLOG_STATUS_PENDING
        return super(AsyncLog, self).save(**kwargs)

    def __str__(self):
        return "%s异步日志%d" % (self.flow, self.id)

    @property
    def finished(self):
        return self.status in [choices.ASYNCLOG_STATUS_ERROR, choices.ASYNCLOG_STATUS_SUCCESS]

    @property
    def async_result(self):
        from celery.result import AsyncResult
        return AsyncResult(self.task_id)

    @property
    def iterdeps(self):
        rs = self.async_result
        ds = []
        for i in rs.iterdeps(intermediate=True):
            crs = i[1]
            ds.append(dict(id=crs.id, state=crs.state, result=unicode(crs.result), traceback=unicode(crs.traceback)))
        return ds

    def terminate_all(self):
        from celery.result import AsyncResult
        rs = self.async_result
        rs.revoke(terminate=True)
        for i in rs.iterdeps(intermediate=True):
            crs = i[1]
            crs.revoke(terminate=True)
        if self.children:
            for k, v in self.children.iteritems():
                AsyncResult(k).revoke(terminate=True)

    def update_task_state(self, task):
        cs = self.children or OrderedDict()
        a = cs.setdefault(task.get("id"), {})
        a.update(task)
        self.affect_records = (self.affect_records or 0) + task.get("affect_records", 0)
        self.children = cs
        self.save()

    def check_state(self):
        rs = self.async_result
        self.status = rs.state == 'PENDING' and choices.ASYNCLOG_STATUS_PENDING \
                      or rs.state in ['STARTED', 'RETRY'] and choices.ASYNCLOG_STATUS_RUNNING \
                      or self.status \
                      or choices.ASYNCLOG_STATUS_RUNNING
        all_done = True
        cs = self.children or OrderedDict()
        has_faield = rs.failed()
        affect_records = 0
        if not self.end_time and (rs.failed() or rs.successful()):
            self.end_time = datetime.now()
        last_end_time = self.end_time
        for i in rs.iterdeps(intermediate=True):
            crs = i[1]
            a = cs.setdefault(crs.id, {})
            a.update(dict(
                id=crs.id,
                state=crs.state,
                result=unicode(crs.result),
                traceback=crs.traceback and unicode(crs.traceback)
            ))
            affect_records += a.get("affect_records", 0)
            end_time = a.get("end_time")
            end_time = end_time and datetime.strptime(end_time[:19], '%Y-%m-%dT%H:%M:%S')
            if end_time and (last_end_time is None or last_end_time < end_time):
                last_end_time = end_time
            if not crs.failed() and not crs.successful():
                all_done = False
                has_faield = has_faield or crs.failed()

        if all_done:
            self.total_end_time = last_end_time
            self.status = has_faield and choices.ASYNCLOG_STATUS_ERROR or choices.ASYNCLOG_STATUS_SUCCESS
            self.cost_seconds = (self.total_end_time - self.end_time).seconds if self.total_end_time else None
        self.affect_records = affect_records
        self.children = cs
        self.save()

        # def update_child(self, task_id):
        #     self.check_state()


class ODSTask(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = 'ODS同步任务'

    name = models.CharField('名称', max_length=64)
    description = models.TextField('简介', null=True, blank=True, default="")
    source = models.ForeignKey("etl.source", verbose_name="源库", related_name='source_odstasks')
    destination = models.ForeignKey("etl.source", verbose_name="目标库", related_name='destination_odstasks')
    source_schema = models.CharField('源库Schema', max_length=64, null=True, blank=True)
    destination_schema = models.CharField('目标库Schema', max_length=64, null=True, blank=True)
    source_timestamp_field = models.CharField('源库timesamp字段', max_length=64)
    # tables = JSONField('表清单', blank=True, null=True, help_text="例子: {'tablename':{'interval':'2m','primary_key':''}}")
    project = models.ForeignKey(Project, verbose_name=Project._meta.verbose_name, null=True, blank=True)
    queue = models.ForeignKey("asynctask.Queue", verbose_name="通道", null=True, blank=True,
                              related_name="warehouse_odstasks")

    def __str__(self):
        return "ODS同步任务:%s" % self.name

    # def save(self, **kwargs):
    #     if self.tables is None:
    #         self.tables = {}
    #
    #     for k, v in self.tables.iteritems():
    #         v['name'] = k
    #         if not v.get('primary_key'):
    #             v['primary_key'] = self.get_table_primary_key(v)
    #         v['primary_key'] = v.get('primary_key', "").lower()
    #         v.setdefault('interval', "1d@h1")
    #     super(ODSTask, self).save(**kwargs)

    # def transfer_old(self):
    #     for k, v in self.tables.iteritems():
    #         table, created = self.ods_tables.update_or_create(
    #             name=k,
    #             defaults=dict(
    #                 primary_key=v.get('primary_key'),
    #                 interval=v.get('interval'),
    #                 write_mode=v.get('write_mode'),
    #                 source_timestamp_field=v.get('source_timestamp_field')
    #             )
    #         )
    #         table.create_flow()

    def create_tables(self):
        for table in self.ods_tables.all():
            try:
                table.create_table()
            except Exception, e:
                log.error("ODS同步任务%s建表%s失败:%s", self, table.name, e)

    def count_tables(self):
        d = {}
        for table in self.ods_tables.all():
            d[table.name] = table.count()
        return d

    def create_flows(self):
        for table in self.ods_tables.all():
            table.create_flow()


class ODSTable(models.Model):
    class Meta:
        verbose_name_plural = verbose_name = 'ODS表'
        unique_together = ('task', 'name')

    task = models.ForeignKey(ODSTask, verbose_name=ODSTask._meta.verbose_name, related_name="ods_tables")
    name = models.CharField('名称', max_length=128)
    code = models.CharField('代号', max_length=64, blank=True, null=True)
    flow = models.OneToOneField(Flow, verbose_name=Flow._meta.verbose_name, null=True, related_name="ods_table",
                                on_delete=models.SET_NULL)
    interval = models.CharField('同步周期', max_length=16, choices=choices.CHOICES_INTERVAL, blank=True, null=True,
                                default="1d@h1")
    primary_key = models.CharField('主键', null=True, blank=True, max_length=128, help_text="如果是多字段复合主键请用逗号','分隔")
    source_timestamp_field = models.CharField('源库timestamp字段', max_length=64, null=True, blank=True,
                                              help_text="'':继承自Task, '-':不使用时间条件(即全表查询),支持用'|'指定时间格式化, 如:opendt|%Y%m%d")
    write_mode = models.PositiveSmallIntegerField('同步模式', null=True, choices=choices.CHOICES_WRITE_MODE,
                                                  default=choices.WRITE_MODE_DELTA_ASYNC,
                                                  help_text="同步增量:单进程串行插入,全量替换:先truncate再插入")
    flow_mode = models.PositiveSmallIntegerField('数据流模式', null=True, choices=choices.CHOICES_FLOW_MODE,
                                                 default=choices.FLOW_MODE_CURSOR)
    on_duplicate = models.CharField('去重模式', max_length=16, blank=True, choices=choices.CHOICES_ON_DUPLICATE_DO,
                                    default=choices.ON_DUPLICATE_DO_REPLACE)
    has_delete_action = models.BooleanField('有删除动作', default=False, blank=True)
    stats = JSONField('统计', null=True, blank=True)

    def __str__(self):
        return "ODS表:%s" % self.name

    def save(self, **kwargs):
        if not self.primary_key:
            self.primary_key = self.get_primary_key()
        if self.primary_key:
            self.primary_key = self.primary_key.lower().replace(' ', '')
        if self.write_mode is None:
            self.write_mode = choices.WRITE_MODE_DELTA_ASYNC
        if self.flow_mode is None:
            self.flow_mode = choices.FLOW_MODE_CURSOR
        if self.on_duplicate is None:
            self.on_duplicate = choices.ON_DUPLICATE_DO_REPLACE
        if not self.code:
            self.code = "%s.%s" % (self.task.name, self.name)
        super(ODSTable, self).save(**kwargs)

    @cached_property
    def source_table_name(self):
        shema = self.task.source_schema
        return "%s.%s" % (shema, self.name) if shema else self.name

    @cached_property
    def destination_table_name(self):
        shema = self.task.destination_schema
        return "%s.%s" % (shema, self.name.lower()) if shema else self.name.lower()

    @cached_property
    def destination_update_timestamp_field(self):
        return self.task.project and self.task.project.update_timestamp_field

    @cached_property
    def source_timestamp_field_parts(self):
        parts = (self.source_timestamp_field or self.task.source_timestamp_field).split('|')
        return parts[0], parts[1] if len(parts) > 1 else None

    @cached_property
    def the_source_timestamp_field(self):
        return self.source_timestamp_field_parts[0]

    @cached_property
    def source_timestamp_field_format(self):
        return self.source_timestamp_field_parts[1]

    def get_max_timestamp(self):
        if self.source_timestamp_field == '-':
            return datetime.now()
        sql = "select max(%s) as mt, 1 as none from %s" % (self.the_source_timestamp_field, self.name)
        #  '1 as none' 只为占位, 以防postgresql timestamp with time zone类型的时间字段导致下面的 .loc[0][0]出现下标错误
        from xyz_util import pandasutils
        df = pandasutils.tz_convert(self.task.source.query(sql))
        d = df.loc[0][0]
        if self.source_timestamp_field_format:
            d = datetime.strptime(d, self.source_timestamp_field_format)
        return d

    def get_primary_key(self):
        from xyz_util import dbutils
        schema = dbutils.get_table_schema(self.task.source.connection, self.name, schema=self.task.source_schema)
        primary_key_columns = schema.get("primary_key_columns")
        if primary_key_columns:
            return ",".join(primary_key_columns)
        unique_columns_groups = schema.get("unique_columns_groups")
        if unique_columns_groups:
            return ",".join(unique_columns_groups[0])

    def create_table(self):
        pks = self.get_primary_key()
        if pks and pks != self.primary_key:
            log.error("%s primary key changed: %s => %s, please remember to alter table change primary key manually.",
                      self, self.primary_key, pks)
            self.primary_key = pks
            self.save()

        from xyz_util import dbutils
        fields = dbutils.get_table_fields(self.task.source.connection, self.name, schema=self.task.source_schema)
        dutf = self.destination_update_timestamp_field
        indexes = None
        if dutf:
            fields[dutf] = dict(name=dutf, label=dutf, comment='记录更新时间', type='DateTimeField', params={}, notes=[])
            indexes = [[dutf]]
        dbutils.create_table(
            self.task.destination.connection,
            self.name.lower(),
            fields,
            schema=self.task.destination_schema,
            force_lower_name=True,
            primary_key=self.primary_key,
            indexes=indexes
        )

    def sync_all_data(self, freq='D', begin_time=None, end_time=None):
        import pandas as pd
        begin_time = pd.to_datetime(begin_time)
        end_time = pd.to_datetime(end_time)
        if self.source_timestamp_field == "-":
            begin_time = datetime(1970, 1, 1)
            end_time = datetime.now()
            end_times = [end_time]
        else:
            tsf = self.the_source_timestamp_field
            sql = "select min(%s) as min_time, max(%s) as max_time from %s where %s>'1970-01-01'" % (
                tsf, tsf, self.name, tsf)
            min_time, max_time = self.task.source.query(sql).loc[0]
            min_time, max_time = pd.to_datetime(min_time), pd.to_datetime(max_time)
            begin_time = begin_time if begin_time and begin_time > min_time else min_time
            end_time = end_time if end_time and end_time < max_time else max_time
            end_times = list(pd.date_range(begin_time, end_time, freq=freq, closed='right'))
            if not end_times or end_times[-1] != end_time:
                end_times.append(end_time)
        for d in end_times:
            context = dict(begin_time=begin_time.isoformat(), end_time=d.isoformat())
            context[ASYNC_TASK_QUEUE_CONTEXT] = None
            context[OUTPUT_WRITE_MODE_CONTEXT] = choices.WRITE_MODE_DELTA_SYNC
            self.run_flow(context)
            begin_time = d
        return end_times

    def count(self, estimate=False):
        if estimate:
            task = self.task
            from xyz_util import dbutils
            db_source = task.source
            sql1 = dbutils.get_estimate_count_sql(db_source.type) % dict(table=self.name,
                                                                         schema=task.source_schema or 'public')
            df1 = db_source.query(sql1)
            db_destination = task.destination
            sql2 = dbutils.get_estimate_count_sql(db_destination.type) % dict(table=self.name.lower(),
                                                                              schema=task.destination_schema)
            df2 = db_destination.query(sql2)
            # print sql1, df1
            # print sql2, df2
            return df1.loc[0][0], df2.loc[0][0]
        return self.pair_aggregate("count(1)")

    def max_timestamp(self):
        if self.source_timestamp_field == '-':
            return None, None
        return self.pair_aggregate("max(%s)" % self.the_source_timestamp_field)

    def run_stat(self):
        s = {"stat_time": datetime.now()}
        d = s['count'] = {}
        last_count = self.stats and self.stats.get('count', {}).get('source') or 0
        estimate = last_count == 0 or last_count > 100000000
        # print 'estimate:', estimate
        d['source'], d['destination'] = self.count(estimate=estimate)
        d = s['max_timestamp'] = {}
        d['source'], d['destination'] = self.max_timestamp()
        self.stats = s
        self.save()

    def run_flow(self, context):
        f = self.flow
        if not f:
            self.create_flow()
            f = self.flow
        rs = f.run_async(context, update_break_point=False)
        return rs

    def get_select_sql(self):

        s_table_name = self.source_table_name

        stf = self.the_source_timestamp_field
        if stf != "-":
            cond_str = "where %s > '{{begin_time}}' and  %s <='{{end_time}}'" % (stf, stf)
        else:
            cond_str = ""
        sql = "select * from %s %s" % (s_table_name, cond_str)
        return sql

    def run(self, data, context={}, progress=lambda x: None):
        from xyz_util.dbutils import transfer_table
        from django.template import Template, Context

        end_time = context.get('end_time')
        if not end_time:
            context['end_time'] = self.get_max_timestamp().isoformat()
        if self.source_timestamp_field_format:
            ctx = {}
            ctx.update(context)
            import pandas as pd
            dfnc = lambda a: datetime.strftime(pd.to_datetime(a), self.source_timestamp_field_format)
            ctx['begin_time'] = dfnc(context.get('begin_time'))
            ctx['end_time'] = dfnc(context.get('end_time'))
            ctx = Context(ctx)
        else:
            ctx = Context(context)
        sql = Template(self.get_select_sql()).render(ctx)
        if self.write_mode == choices.WRITE_MODE_REPLACE:
            self.task.destination.execute('truncate table %s' % self.destination_table_name)
        count = transfer_table(self.task.source.connection,
                               self.task.destination.connection,
                               sql,
                               self.destination_table_name,
                               insert_type=self.on_duplicate,
                               primary_keys=self.primary_key,
                               update_timestamp_field=self.destination_update_timestamp_field,
                               lower_field_name=True)
        progress(dict(affect_records=count))
        return count

    def pair_aggregate(self, measure):
        df1, df2 = self.pair_query("%s, 1 as none" % measure)
        # ",1 as none" 只为占位, 以防postgresql timestamp with time zone类型的时间字段导致下面的 .loc[0][0]出现下标错误
        return df1.loc[0][0], df2.loc[0][0]

    def pair_query(self, fields, extra_sql_parts=""):
        task = self.task
        if isinstance(fields, (tuple, list)):
            fields1, fields2 = fields
        else:
            fields1 = fields2 = fields
        df1 = task.source.query("select %s from %s %s" % (fields1, self.source_table_name, extra_sql_parts))
        df2 = task.destination.query("select %s from %s %s" % (fields2, self.destination_table_name, extra_sql_parts))
        return df1, df2

    def compare_and_sync(self, begin_time='1970-01-01', end_time=None):
        import pandas as pd
        from datetime import timedelta
        if self.the_source_timestamp_field == '-':
            return
        df = self.daily_compare(begin_time=begin_time, end_time=end_time)
        diffs = df[df['rec_count_x'] > df['rec_count_y']]
        diffs = diffs.append(df[pd.isnull(df['rec_count_y'])])
        for d in diffs['the_date']:
            if self.source_timestamp_field_format == '%Y%m%d':
                end_time = d + 'T00:00:00'
                begin_time = (pd.to_datetime(end_time) + timedelta(days=-1)).isoformat()
            else:
                begin_time = d + 'T00:00:00'
                end_time = (pd.to_datetime(begin_time) + timedelta(days=1)).isoformat()
            context = dict(begin_time=begin_time, end_time=end_time)
            context[ASYNC_TASK_QUEUE_CONTEXT] = None
            context[OUTPUT_WRITE_MODE_CONTEXT] = choices.WRITE_MODE_DELTA_SYNC
            self.run_flow(context)
        return diffs['the_date'].tolist()

    def difference_primary_keys(self, how='left'):
        df1, df2 = self.pair_query(self.primary_key)
        pks = self.primary_key.split(',')
        df1.set_index(pks, inplace=True)
        df2.set_index(pks, inplace=True)
        return df1.index.difference(df2.index) if how == 'left' else df2.index.difference(df1.index)

    def delete_disappear_records(self):
        ids = self.difference_primary_keys(how='right')
        fs = ids.names
        destination = self.task.destination
        for id in ids:
            id_list = id if isinstance(id, (list, tuple)) else [id]
            cond = " and ".join(["%s = '%s'" % (f, v) for f, v in zip(fs, id_list)])
            sql = "delete from %s where %s" % (self.destination_table_name, cond)
            destination.execute(sql)
        return len(ids)

    def daily_compare(self, begin_time='1970-01-01', end_time=None):
        tsf = self.the_source_timestamp_field
        date_format = lambda e: e == 'postgresql' and "to_char(%s::timestamp,'YYYY-mm-dd')" \
                                or e == 'mysql' and "left(%s,10)" or ""
        dfield1 = date_format(self.task.source.type) % tsf
        dfield2 = date_format(self.task.destination.type) % tsf
        fields_template = "%s as the_date, count(1) as rec_count"
        if self.source_timestamp_field_format:
            import pandas as pd
            dfnc = lambda a: datetime.strftime(pd.to_datetime(a), self.source_timestamp_field_format)
            begin_time = dfnc(begin_time)
            end_time = dfnc(end_time)
        cond = "where %s>='%s'" % (tsf, begin_time)
        if end_time:
            cond = cond + " and %s<'%s'" % (tsf, end_time)
        df1, df2 = self.pair_query((fields_template % dfield1, fields_template % dfield2),
                                   extra_sql_parts="%s group by 1" % cond)
        return df1.merge(df2, how='left', on='the_date')

    def get_primary_keys_cond(self):
        from xyz_util import dbutils
        pks = self.primary_key.split(',')
        fields = dbutils.get_table_fields(self.task.source.connection, self.name, self.task.source_schema)
        pk_conds = []
        for pk in pks:
            ftype = fields.get(pk).get('type')
            if 'Char' in ftype or 'Date' in ftype:
                t = "%s='{{%s}}'"
            else:
                t = "%s={{%s}}"
            pk_conds.append(t % (pk, pk))
        return " and ".join(pk_conds)

    def create_flow(self):
        if self.flow and self.flow.is_active:
            self.flow.is_active = False
            self.flow.save()

        if self.flow_mode == choices.FLOW_MODE_TIME:
            self.create_time_flow()
        elif self.flow_mode == choices.FLOW_MODE_PK:
            self.create_pk_flow()
        else:
            self.create_cursor_flow()

    def create_cursor_flow(self):
        self.flow, created = Flow.objects.update_or_create(
            code=self.code,
            defaults=dict(
                name=self.code,
                content="ODSTable(%s)" % self.code,
                is_active=True,
                interval=self.interval,
                project=self.task.project,
                queue=self.task.queue
            )
        )
        self.save()

    def create_time_flow(self):
        d_table_name = self.destination_table_name
        sql = self.get_select_sql()

        code = "ODS.%s" % d_table_name

        input, created = Input.objects.update_or_create(
            code=code,
            defaults=dict(
                name=code,
                db=self.task.source,
                table_name=sql,
                project=self.task.project
            )
        )

        output, created = Output.objects.update_or_create(
            code=code,
            defaults=dict(
                name=code,
                db=self.task.destination,
                table_name=d_table_name,
                primary_key=self.primary_key,
                write_mode=self.write_mode,
                project=self.task.project
            )
        )

        self.flow, created = Flow.objects.update_or_create(
            code=code,
            defaults=dict(
                name=code,
                content="Input(%s)|Output(%s)" % (input.code, output.code),
                is_active=True,
                interval=self.interval,
                project=self.task.project,
                queue=self.task.queue
            )
        )
        self.save()

    def create_pk_flow(self):
        log.info("create flow for odstable:%s", self)
        s_table_name = self.source_table_name
        d_table_name = self.destination_table_name

        stf = self.the_source_timestamp_field
        if stf != "-":
            cond_str = "where %s > '{{begin_time}}' and  %s <='{{end_time}}'" % (stf, stf)
        else:
            cond_str = ""
        sql = "select %s from %s %s" % (self.primary_key, s_table_name, cond_str)

        code = "ODS.%s" % d_table_name

        input_pk, created = Input.objects.update_or_create(
            code="%s.pk" % code,
            defaults=dict(
                name="%s.pk" % code,
                db=self.task.source,
                table_name=sql,
                project=self.task.project
            )
        )

        sql = "select * from %s where %s" % (s_table_name, self.get_primary_keys_cond())

        input_all, created = Input.objects.update_or_create(
            code=code,
            defaults=dict(
                name=code,
                db=self.task.source,
                table_name=sql,
                project=self.task.project
            )
        )

        output, created = Output.objects.update_or_create(
            code=code,
            defaults=dict(
                name=code,
                db=self.task.destination,
                table_name=d_table_name,
                primary_key=self.primary_key,
                write_mode=self.write_mode,
                project=self.task.project
            )
        )

        flow_all, created = Flow.objects.update_or_create(
            code=code,
            defaults=dict(
                name=code,
                content="Input(%s)|Output(%s)" % (input_all.code, output.code),
                is_active=True,
                interval=choices.INTERVAL_NONE,
                project=self.task.project,
                queue=self.task.queue
            )
        )

        self.flow, created = Flow.objects.update_or_create(
            code="%s.pk" % code,
            defaults=dict(
                name="%s.pk" % code,
                content="Input(%s)|Flow(%s)" % (input_pk.code, flow_all.code),
                is_active=True,
                interval=self.interval,
                project=self.task.project,
                queue=self.task.queue
            )
        )

        self.save()
