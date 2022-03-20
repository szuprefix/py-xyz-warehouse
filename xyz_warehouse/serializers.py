# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from rest_framework import serializers
from . import models
from xyz_restful.mixins import IDAndStrFieldSerializerMixin


class ProjectSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    class Meta:
        model = models.Project
        exclude = ()


class RuleSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    vars = serializers.JSONField(read_only=True)
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)

    class Meta:
        model = models.Rule
        exclude = ()


class InputSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    db_name = serializers.CharField(source="db.__str__", label="源库", read_only=True)
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)

    class Meta:
        model = models.Input
        exclude = ()


class OutputSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    db_name = serializers.CharField(source="db.__str__", label="源库", read_only=True)
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)

    class Meta:
        model = models.Output
        exclude = ()


class SelectExcludeSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    db_name = serializers.CharField(source="db.__str__", label="源库", read_only=True)
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)

    class Meta:
        model = models.SelectExclude
        exclude = ()


class NotifySerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    class Meta:
        model = models.Notify
        exclude = ()


class MapSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    class Meta:
        model = models.Map
        exclude = ()


class FlowSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)
    queue_name = serializers.CharField(source="queue.__str__", label="通道", read_only=True)

    class Meta:
        model = models.Flow
        exclude = ()


class AsyncLogSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    flow_name = serializers.CharField(source="flow.name", label="数据流", read_only=True)
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)
    status_name = serializers.CharField(source="get_status_display", label="状态")

    class Meta:
        model = models.AsyncLog
        exclude = ()


class ODSTaskSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    source_name = serializers.CharField(source="source.__str__", label="源库", read_only=True)
    destination_name = serializers.CharField(source="destination.__str__", label="目标库", read_only=True)
    project_name = serializers.CharField(source="project.__str__", label="项目", read_only=True)
    queue_name = serializers.CharField(source="queue.__str__", label="通道", read_only=True)

    class Meta:
        model = models.ODSTask
        exclude = ()


class ODSTableSerializer(IDAndStrFieldSerializerMixin, serializers.ModelSerializer):
    task_name = serializers.CharField(source="task.__str__", label="任务", read_only=True)

    class Meta:
        model = models.ODSTable
        exclude = ()
        read_only_fields = ('stats',)
