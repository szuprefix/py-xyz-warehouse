# -*- coding:utf-8 -*-
from django.contrib import admin

# Register your models here.

from . import models, helper


class InputAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'db')


admin.site.register(models.Input, InputAdmin)


class OutputAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'db')


admin.site.register(models.Output, OutputAdmin)


class RuleAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'caltype')


admin.site.register(models.Rule, RuleAdmin)


class SelectExcludeAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'db')


admin.site.register(models.SelectExclude, SelectExcludeAdmin)


class FlowAdmin(admin.ModelAdmin):
    list_display = ('name', 'code', 'content')


admin.site.register(models.Flow, FlowAdmin)


class DataCompareAdmin(admin.ModelAdmin):
    list_display = ('name', 'code')
    search_fields = ('name', 'code')


admin.site.register(models.DataCompare, DataCompareAdmin)


class AsyncLogAdmin(admin.ModelAdmin):
    list_display = ('task_id', 'flow', 'create_time', 'begin_time', 'end_time', 'total_end_time')
    raw_id_fields = ("flow",)


admin.site.register(models.AsyncLog, AsyncLogAdmin)


class ODSTaskAdmin(admin.ModelAdmin):
    list_display = ('name', 'description', 'source', 'destination')


admin.site.register(models.ODSTask, ODSTaskAdmin)


class ODSTableAdmin(admin.ModelAdmin):
    list_display = ('name', 'task', 'interval', 'primary_key', 'write_mode', 'source_timestamp_field')


admin.site.register(models.ODSTable, ODSTableAdmin)
