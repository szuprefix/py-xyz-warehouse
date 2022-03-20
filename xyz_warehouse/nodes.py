# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals, print_function

from xyz_util.datautils import str2dict
from . import mixins, models
import logging

log = logging.getLogger("django")


class BaseNode(mixins.IORunable):
    @classmethod
    def is_instance(self, s):
        if not hasattr(self, 'node'):
            self.node = self.__name__
        if not hasattr(self, 'default_search_field'):
            self.default_search_field = 'code'
        s = s.strip()
        if s.startswith("%s(" % self.node) and s.endswith(")"):
            p = len(self.node) + 1
            args = [s[p:-1].strip()]
            o = self.__new__(self, *args)
            o.__init__(*args)
            return o

    def __init__(self, s):
        self.body = s
        if ":" in s:
            self.kwargs = str2dict(s, line_spliter=" ")
        else:
            self.kwargs = {self.default_search_field: s}

    def __str__(self):
        return "%s('%s')" % (self.node, self.body)


class BaseModelNode(BaseNode):
    def get_object(self):
        return self.model.objects.get(**self.kwargs)

    def run(self, data, context={}, progress=lambda x: None):
        n = self.get_object()
        return n.run(data, context=context, progress=progress)


class ODSTable(BaseModelNode):
    model = models.ODSTable


class Input(BaseModelNode):
    model = models.Input


class Output(BaseModelNode):
    model = models.Output


class Rule(BaseModelNode):
    model = models.Rule


class Notify(BaseModelNode):
    model = models.Notify


class SelectExclude(BaseModelNode):
    model = models.SelectExclude


class Filter(BaseNode):
    def run(self, data, context={}, progress=lambda x: None):
        d = self.kwargs
        for k, v in d.items():
            if v:
                data[k] = data[v]
        return data[d.keys()]


class Flow(BaseModelNode):
    model = models.Flow

    def run(self, data, context={}, progress=lambda x: None):
        from . import models, choices
        from xyz_util.pandasutils import split_dataframe_into_chunks
        f = self.get_object()
        for df in split_dataframe_into_chunks(data, models.OUTPUT_BATCH_SIZE):
            ctx = {'batch_primary_keys': df.to_dict("records"),
                   models.OUTPUT_WRITE_MODE_CONTEXT: choices.WRITE_MODE_DELTA_SYNC}
            ctx.update(context)
            log.info("start sub flow: %s", f)
            f.run_async(context=ctx, update_break_point=False)


class Dict(BaseNode):
    def run(self, data, context={}, progress=lambda x: None):
        kws = self.kwargs
        from xyz_etl.models import Dictionary
        ds = {}
        for d in Dictionary.objects.all():
            ds.update(d.structure)
        print(ds.keys())
        for fn, dn in kws.iteritems():
            ps = fn.split(">")
            cfn = ps[0]
            nfn = ps[1] if len(ps) > 1 else "%s_name" % cfn
            print(cfn, nfn, dn)
            d = ds[dn or fn]
            cases = d.get("cases")
            data[nfn] = data[cfn].apply(lambda x: cases.get(unicode(x)))
        return data


def check_node(s):
    return Input.is_instance(s) or \
           Output.is_instance(s) or \
           ODSTable.is_instance(s) or \
           Rule.is_instance(s) or \
           Dict.is_instance(s) or \
           Filter.is_instance(s) or \
           Flow.is_instance(s) or \
           SelectExclude.is_instance(s) or \
           Notify.is_instance(s) or None
    # Map.is_instance(s) or
