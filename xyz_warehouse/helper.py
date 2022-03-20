# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from collections import OrderedDict

from django.core.files.base import ContentFile
from xyz_util.datautils import re_group_split, str2dict, try_numeric_dict
import re

RE_CALCULATE = re.compile(r"([+*/-])")
RE_SPACES = re.compile(r"\s+")


def is_numeric(s):
    try:
        float(s)
    except Exception, e:
        return False

    return True


class BaseToken(object):
    @classmethod
    def is_instance(self, s):
        if not hasattr(self, 'token'):
            self.token = self.__name__
        s = s.strip()
        if s.startswith("%s(" % self.token) and s.endswith(")"):
            p = len(self.token) + 1
            args = [s[p:-1].strip()]
            o = self.__new__(self, *args)
            o.__init__(*args)
            return o

    def get_eval(self):
        return "lambda x: x"

    def __init__(self, s):
        self.body = s

    def __str__(self):
        return "%s('%s')" % (self.token, self.body)

    def run(self):
        pass

    def __mul__(self, other):
        return Float(self.run(other))


class Rank(BaseToken):
    def __init__(self, s):
        super(Rank, self).__init__(s)
        d = try_numeric_dict(str2dict(self.body, line_spliter=" "))
        self.round = 2
        self.kwargs = {"ascending": False}
        for k, v in d.items():
            if k in ["降序", "升序"]:
                self.kwargs["ascending"] = k == "升序" or False
            elif k in ["百分比", "%"]:
                self.kwargs["pct"] = True
            elif k in ["小数"]:
                self.round = v

    def __mul__(self, other):
        return other.rank(**self.kwargs).round(self.round)


class RankRate(Rank):
    def __init__(self, s):
        super(RankRate, self).__init__(s)
        self.kwargs = str2dict(self.body, line_spliter=" ")
        self.kwargs['pct'] = True


class Condition(BaseToken):
    """
    s="{[0,0.1)=10;[0.1,0.2)=20;[0.2,inf)=30}"
    c=Condition(s)
    c*0.08  #  10
    c*0.19999  #  20
    c*0.2   #30
    """

    def get_eval(self, s):
        if not s:
            return
        return s.replace(";", " or ") \
            .replace(",", " and ") \
            .replace("=", " and ") \
            .replace("[", "x>=") \
            .replace("(", "x>") \
            .replace("]", ">=x") \
            .replace(")", ">x") \
            .replace("inf", "float('inf')")

    # @classmethod
    # def is_instance(self, s):
    #     return s.startswith("{") and s.endswith("}")

    def __init__(self, s):
        super(Condition, self).__init__(s)
        self.func = self.body and eval("lambda x: %s or 0" % self.get_eval(self.body)) or (lambda x: x)

    def __mul__(self, other):
        import pandas as pd
        if isinstance(other, pd.Series):
            return other.apply(self.func)
        return Float(self.func(other))


# class Rank(object):
#     def __init__(self, f):
#
#     def __mul__(self, other):
#         return other.rank()
#
# class RankRate(object):
#     def __mul__(self, other):
#         return other.rank() / other.count()

class BasePine(object):
    Token = "Filter"

    @classmethod
    def is_instance(self, s):
        return s.startswith("%s{" % self.Token) and s.endswith("}")

    def get_eval(self):
        return "lambda x: x"

    def __init__(self, s):
        s = s.strip()
        p = len("%s{" % self.Token)
        self.body = s[p:-1]

    def run(self, input):
        return input


class RankPine(BasePine):
    Token = "Rank"

    def __init__(self, s):
        super(RankPine, self).__init__(s)
        self.field_map = str2dict(self.body)

    def get_eval(self):
        return "lambda x: x[%s].rank()" % self.field

    def run(self, input):
        for k, v in self.field_map.items():
            pass
        return input


class Float(float):
    def __mul__(self, other):
        import pandas as pd
        if isinstance(other, pd.Series):
            return other.apply(lambda x: Float(x * self))
        elif isinstance(other, Condition):
            a = other * self
        else:
            a = super(Float, self).__mul__(other)
        return Float(a)


class Equation(object):
    """
    e=Equation(u"坐席得分 = 老客资金续存率排名得分 * 0.2 + 新客资金规模排名得分 * 0.15 + 客均交易资金规模区间得分 * 0.1 + 话术通关得分 * 0.1 + 主管听录音得分 * 0.1")
    e=Equation(u"排名得分规则 = Condition([0,0.1)=100;[0.1,0.2)=90;[0.2,0.3)=80;[0.3,0.5)=70;[0.5,0.7)=60;[0.7,1)=50)")
    """

    def __init__(self, s):
        self.source = s
        ei = s.index("=")
        self.name = s[:ei].strip()
        self.value = v = s[ei + 1:].strip()
        self.dependants = set()
        es = []
        for gs, a in re_group_split(RE_CALCULATE, self.value):
            op = gs and gs[0]
            if op:
                es.append(op)
            a = a.strip()
            e = self.is_token(a) or a
            if isinstance(e, (str, unicode)) and not is_numeric(e):
                self.dependants.add(e)
            es.append(e)
        self.elements = es

        self.is_token_equation = self.is_token(self.value)

    def is_token(self, s):
        return Condition.is_instance(s) or Rank.is_instance(s) or RankRate.is_instance(s)

    def get_eval_str(self, map_name="x", token_list=[]):
        # if self.is_condition:
        #     return "Condition('%s')" % self.value
        pas = []
        op = None
        for e in self.elements:
            is_token = isinstance(e, BaseToken)
            if is_token:
                c = e
            elif e in '*/+-':
                op = e
                c = e
            else:
                c = is_numeric(e) and e or "%s[u'%s']" % (map_name, e)
            if e != "*" and op == "*":
                tmp = pas[-2]
                del pas[-2:]
                change_order = is_token or e in token_list
                c = "(%s * %s)" % (change_order and (c, tmp) or (tmp, c))
                # print c
            pas.append(c)
        return "".join([unicode(p) for p in pas])

    def __str__(self):
        return self.source


class Rule(object):
    """

      rule=u'''近两月新客资金规模=近两月新客资金规模排名*Rank()
      排名得分规则 = Condition([0,0.1)=100;[0.1,0.2)=90;[0.2,0.3)=80;[0.3,0.5)=70;[0.5,0.7)=60;[0.7,1)=50)
      老客资金续存率排名得分 = 近两月老客资金续存率排名 * 排名得分规则
      新客资金规模排名得分 = 近两月新客资金规模排名 * 排名得分规则
      资金规模区间得分规则 = Condition([0, 20000)=60;[20000,40000)=70;[40000,60000)=80;[60000,80000)=90;[80000,100000)=95;[100000,inf)=100)
      客均交易资金规模区间得分 = 近两个月客均交易资金规模区间(交易客户)*资金规模区间得分规则
      话术通关得分区间规则 = Condition([0,60]=60;(60,80]=70;(80,95]=80;(95,100]=100)
      话术通关得分 = 近两个月月度话术通关得分区间 * 话术通关得分区间规则
      主管听录音得分规则 = Condition([0,60]=60;(60,80]=80;(80,100]=100)
      主管听录音得分 = 主管(或质检)听录音判断线上应用效果 * 主管听录音得分规则
      坐席得分 = 老客资金续存率排名得分 * 0.2 + 新客资金规模排名得分 * 0.15 + 客均交易资金规模区间得分 * 0.1 + 话术通关得分 * 0.1 + 主管听录音得分 * 0.1
      '''

      r=Rule(rule)

      d={u"最大保有额度":2000,u"排名":0.19}
      r.run(d)

      df=pd.DataFrame({u"座席姓名":[u'斯文',u'家军',u'金城',u'灿城'],u"近两月新客资金规模排名":[0.08,0.1,0.7,0.99],u"近两个月月度话术通关得分区间":[100,80,60,100],u"近两月老客资金续存率排名":[0.1,0.5,0.9,1]})
      r.run(df)
    """

    def __init__(self, s):
        self.source = s
        es = []
        for l in s.split("\n"):
            l = l.strip()
            if not l:
                continue
            es.append(Equation(l))
        self.equations = es
        self.vars = set([e.name for e in es])
        self.check_dependants()

    def check_dependants(self):
        ds = set()
        for e in self.equations:
            ds.update(e.dependants)
        self.inputs = ds.difference(self.vars)
        self.outputs = set([e.name for e in self.equations if not e.is_token_equation])
        self.tokens = set([e.name for e in self.equations if e.is_token_equation])
        dss = Dependant(self.dependants).order
        self.eval_strs = [(e.name, e.get_eval_str(token_list=self.tokens)) for e in self.equations if
                          e.is_token_equation]
        self.eval_strs += [(d, self.equations_map[d].get_eval_str(token_list=self.tokens)) for d in dss if
                           d in self.equations_map]

    def run(self, x):
        for i in self.inputs:
            if i not in x:
                x[i] = 0

        for n, e in self.eval_strs:
            x[n] = eval(e)
        for c in self.tokens:
            x.pop(c)
        return x

    @property
    def dependants(self):
        if not hasattr(self, "_dependants"):
            ns = self.inputs.union(self.outputs)
            rs = []
            for e in self.equations:
                for d in e.dependants:
                    if d in ns:
                        rs.append((d, e.name))
            self._dependants = rs
        return self._dependants

    @property
    def equations_map(self):
        if not hasattr(self, "_equations_map"):
            self._equations_map = dict([(e.name, e) for e in self.equations])
        return self._equations_map

    def __str__(self):
        return self.source


class Map(object):
    def __init__(self, s, output_field=None):
        import pandas as pd
        df = pd.read_csv(ContentFile(s.strip()), sep='\s+', encoding='utf8')
        if output_field is None:
            output_field = df.columns[-1]
        input_fields = [c for c in df.columns if c != output_field]
        conds = ' & '.join(["(x[u'%s'] == y[u'%s'])" % (i, i) for i in input_fields])
        self.func_str = "lambda x,y: x.loc[%s][u'%s']" % (conds, output_field)
        self.func = eval(self.func_str)
        self.data = df
        self.output_field = output_field
        self.input_fields = input_fields
        self.outputs = [self.output_field]
        self.inputs = self.input_fields

    def search(self, series):
        s = self.func(self.data, series)
        return not s.empty and s.iloc[0] or None

    def run(self, data):
        data[self.output_field] = [self.search(data.loc[i]) for i in data.index]
        return data


"""

dependenses=[('a', 'g'),
 ('a', 'c'),
 ('a', 'd'),
 ('b', 'c'),
 ('d', 'b'),
 ('d', 'e'),
 ('f', 'e'),
 ('g', 'f'),
 ('b', 'g')]

 ds=Dependant(dependenses)

 ds.order
>>['a', 'd', 'b', 'g', 'f', 'e', 'c']

  ds.paths
>>{'a': [['a', 'g', 'f'],
  ['a'],
  ['a', 'd', 'b'],
  ['a', 'd', 'b', 'g', 'f'],
  ['a', 'd']],
 'b': [['b'], ['b', 'g', 'f']],
 'd': [['d', 'b'], ['d', 'b', 'g', 'f'], ['d']],
 'f': [['f']],
 'g': [['g', 'f']]}

  ds.effects
 >>{'a': {'a', 'b', 'd', 'f', 'g'},
 'b': {'b', 'f', 'g'},
 'd': {'b', 'd', 'f', 'g'},
 'f': {'f'},
 'g': {'f', 'g'}}


"""

class Dependant(object):

    def __init__(self, dependenses):
        self.dependenses = dependenses
        self.map, self.errors, self.paths = self.detect_circle(dependenses)
        if self.errors:
            raise Exception("检测到有依赖死循环, %s" % self.errors)
        self.order = self.sort(self.dependenses)
        self.effects = self.detect_effects()

    def detect_effects(self):
        d = {}
        for k ,v  in self.paths.iteritems():
            d[k] = set()
            for p in v:
                d[k].update(p)
        return d

    def sort(self, ds):
        sa = set()
        sb = set()
        for a, b in ds:
            sa.add(a)
            sb.add(b)
        sc = list(sa.difference(sb))
        remain = [(a, b) for a, b in ds if a not in sc]
        if remain:
            sc += self.sort(remain)
        sc += list(set([b for a, b in ds if a in sc and b not in sa and b not in sc]))
        return sc

    def detect_circle(self, ds):
        map = {}
        paths = {}
        for a, b in ds:
            map.setdefault(a, OrderedDict())
            map[a][b] = map.get(b)
        for k, v in map.iteritems():
            for k2, v2 in v.iteritems():
                map[k][k2] = map.get(k2)
        errors = set()
        for k, v in map.iteritems():
            try:
                paths[k] = self.navigate(map[k], k, [k])
            except Exception, e:
                errors.add(str(e))
        return map, errors, paths


    def navigate(self,m, target, paths=[]):
        ps = []
        for k, v in m.iteritems():
            if k in paths:
                p = paths.index(k)
                raise Exception("->".join(paths[p:] + [k]))
            if v:
                ps += self.navigate(v, target, paths + [k])
            else:
                ps.append(paths)
        return ps



def get_default_begin_and_end_time():
    from xyz_util import dateutils
    begin_time = dateutils.get_next_date(None, -1).isoformat()
    end_time = dateutils.format_the_date().isoformat()
    return dict(begin_time=begin_time, end_time=end_time)



#xia 20180820 增加
import logging

log = logging.getLogger("django")
from django.core.mail import EmailMessage
import tempfile
def mail_data_to_datacompare(data, subject,body={}, to=None, cc=None):

    if len(data)==0:
        return 
    text_content = '数据总量不匹配！请核对当天数据！'
    email = EmailMessage(subject=subject, body=text_content, to=to, cc=cc)
    fname = tempfile.mktemp(".xlsx")
    data['dataframe'].to_excel(fname, index=False)
    email.attach(data['filename'], open(fname).read(), 'application/vnd.ms-excel; charset=utf8')
    email.send()
