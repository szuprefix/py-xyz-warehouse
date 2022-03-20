# -*- coding:utf-8 -*-
__author__ = 'denishuang'
from rest_framework.response import Response

from . import serializers, models
from rest_framework import viewsets, status
from rest_framework.decorators import action
from xyz_restful.decorators import register
from xyz_util import pandasutils


@register()
class ProjectViewSet(viewsets.ModelViewSet):
    queryset = models.Project.objects.all()
    serializer_class = serializers.ProjectSerializer
    search_fields = ('name',)


@register()
class RuleViewSet(viewsets.ModelViewSet):
    queryset = models.Rule.objects.all()
    serializer_class = serializers.RuleSerializer
    search_fields = ('name', 'code')
    filter_fields = {
        'project': ['exact'],
        'code': ['exact']
    }

    @action(['post'], detail=True, permission_classes=[])
    def test(self, request, pk=None):
        rule = self.get_object()
        return Response(rule.run_test())


@register()
class InputViewSet(viewsets.ModelViewSet):
    queryset = models.Input.objects.all()
    serializer_class = serializers.InputSerializer
    search_fields = ('name', 'code', 'table_name')
    filter_fields = ('db', 'project', 'code')

    @action(['get'],detail=True, permission_classes=[])
    def test(self, request, pk=None):
        try:
            input = self.get_object()
            ps = request.query_params
            context = {'begin_time': ps.get('begin_time'), 'end_time': ps.get('end_time')}
            data = pandasutils.dataframe_to_table(input.run({}, context), is_preview=True)
        except Exception, e:
            data = {'error': unicode(e)}
        return Response(data)


@register()
class OutputViewSet(viewsets.ModelViewSet):
    queryset = models.Output.objects.all()
    serializer_class = serializers.OutputSerializer
    search_fields = ('name', 'code', 'table_name')
    filter_fields = {'db': ['exact'], 'write_mode': ['exact', 'in'], 'project': ['exact'], 'code': ['exact']}

    @action(['post'], detail=True, permission_classes=[])
    def upload(self, request, pk=None):
        try:
            output = self.get_object()
            file_obj = request.FILES.get('file', None)
            import pandas as pd
            df = pd.read_excel(file_obj)
            output.run(df)
            data = {"count": len(df)}
        except Exception, e:
            data = {'error': unicode(e)}
        return Response(data)


@register()
class NotifyViewSet(viewsets.ModelViewSet):
    queryset = models.Notify.objects.all()
    serializer_class = serializers.NotifySerializer
    search_fields = ('name', 'code', 'api')
    filter_fields = ('code',)


@register()
class SelectExcludeViewSet(viewsets.ModelViewSet):
    queryset = models.SelectExclude.objects.all()
    serializer_class = serializers.SelectExcludeSerializer
    search_fields = ('name', 'code',)
    filter_fields = ('code',)


@register()
class FlowViewSet(viewsets.ModelViewSet):
    queryset = models.Flow.objects.all()
    serializer_class = serializers.FlowSerializer
    search_fields = ('name', 'code', 'content')
    filter_fields = {
        'interval': ['exact', 'in'],
        'is_active': ['exact'],
        'project': ['exact'],
        'queue': ['exact'],
        'code': ['exact']
    }
    ordering_fields = ('break_point', 'create_time')

    @action(['post'], detail=True)
    def run(self, request, pk=None):
        flow = self.get_object()
        context = request.data.get('context')
        context[models.FLOW_TASK_LINES_CONTEXT] = context.pop('content')
        # if flow.is_running:
        #     return Response(dict(detail="还有任务正在执行中"), status=status.HTTP_429_TOO_MANY_REQUESTS)
        frequency = context.pop('frequency', None)
        if frequency:
            import pandas as pd
            begin_time = pd.to_datetime(context.get('begin_time'))
            end_time = pd.to_datetime(context.get('end_time'))
            end_times = list(pd.date_range(begin_time, end_time, freq=frequency, closed='right'))
            if not end_times or end_times[-1] != end_time:
                end_times.append(end_time)
            for end_time in end_times:
                context['begin_time'] = begin_time
                context['end_time'] = end_time
                rs = flow.run_async(context, update_break_point=False)
                begin_time = end_time
        else:
            rs = flow.run_async(context, update_break_point=False)
        return Response(dict(task=dict(id=rs.id, status=rs.status)), status=status.HTTP_201_CREATED)

    @action(['get'], detail=True)
    def test_all(self, request, pk=None):
        flow = self.get_object()
        qs = request.query_params
        context = {'begin_time': qs.get("begin_time"), 'end_time': qs.get("end_time")}
        flow.run(context)
        return Response({})

    @action(['post'], detail=True)
    def terminate_all(self, request, pk=None):
        flow = self.get_object()
        res = flow.terminate_all()
        return Response(dict(async_logs=res))


@register()
class AsyncLogViewSet(viewsets.ModelViewSet):
    queryset = models.AsyncLog.objects.all()
    serializer_class = serializers.AsyncLogSerializer
    search_fields = ('flow__name', 'flow__code', 'task_id')
    filter_fields = {
        'flow': ['exact'],
        'status': ['exact', 'in'],
        'project': ['exact']
    }
    ordering_fields = ('flow', 'status', 'cost_seconds', 'affect_records')

    @action(['post'], detail=True, permission_classes=[])
    def terminate_all(self, request, pk=None):
        alog = self.get_object()
        alog.terminate_all()
        return Response(dict(result=alog.iterdeps))


@register()
class ODSTaskViewSet(viewsets.ModelViewSet):
    queryset = models.ODSTask.objects.all()
    serializer_class = serializers.ODSTaskSerializer
    search_fields = ('name',)
    filter_fields = {
        'project': ['exact'],
        'queue': ['exact'],
        'source': ['exact', 'in'],
        'destination': ['exact', 'in']
    }

    @action(['post'], detail=True)
    def create_flows(self, request, pk=None):
        task = self.get_object()
        task.create_tables()
        task.create_flows()
        return Response(serializers.ODSTableSerializer(task.ods_tables, many=True).data, status=status.HTTP_201_CREATED)

    @action(['post'], detail=True)
    def create_tables(self, request, pk=None):
        task = self.get_object()
        task.create_tables()
        return Response(serializers.ODSTableSerializer(task.ods_tables, many=True).data, status=status.HTTP_201_CREATED)

    @action(['post'], detail=True)
    def count_tables(self, request, pk=None):
        task = self.get_object()
        d = task.count_tables()
        return Response(dict(tables=d))


@register()
class ODSTableViewSet(viewsets.ModelViewSet):
    queryset = models.ODSTable.objects.all()
    serializer_class = serializers.ODSTableSerializer
    search_fields = ('name',)
    filter_fields = {
        "task": ['exact'],
        "code": ['exact'],
        "write_mode": ['exact', 'in'],
        "interval": ['exact', 'in'],
        "flow_mode": ['exact', 'in'],
        "on_duplicate": ['exact', 'in']
    }

    @action(['post'], detail=True)
    def create_table(self, request, pk=None):
        table = self.get_object()
        table.create_table()
        return Response(dict(detail="success"))

    @action(['post'], detail=True)
    def create_flow(self, request, pk=None):
        table = self.get_object()
        table.create_table()
        table.create_flow()
        return Response(dict(detail="success"))

    @action(['post'], detail=True)
    def run_stat(self, request, pk=None):
        table = self.get_object()
        table.run_stat()
        return Response(serializers.ODSTableSerializer(table).data)

    @action(['post'], detail=True)
    def sync_all_data(self, request, pk=None):
        table = self.get_object()
        d = request.data
        if d.get('mode') == 'compare':
            r = table.compare_and_sync(begin_time=d.get("begin_time"), end_time=d.get("end_time"))
        else:
            r = table.sync_all_data(freq=d.get("freq"), begin_time=d.get("begin_time"), end_time=d.get("end_time"))
        return Response(dict(detail="success", datetime_list=r))
