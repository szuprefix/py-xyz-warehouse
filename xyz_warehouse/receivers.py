# -*- coding:utf-8 -*- 
# author = 'denishuang'
from __future__ import unicode_literals

from django.db.models.signals import pre_save
from django.dispatch import receiver
from . import models
from xyz_common.signals import to_save_version


@receiver(pre_save, sender=models.Flow)
def save_flow_version(sender, **kwargs):
    to_save_version.send_robust(sender, instance=kwargs['instance'], exclude_fields=['break_point', 'result_code'])

@receiver(pre_save, sender=models.Input)
def save_input_version(sender, **kwargs):
    to_save_version.send_robust(sender, instance=kwargs['instance'])

@receiver(pre_save, sender=models.Rule)
def save_rule_version(sender, **kwargs):
    to_save_version.send_robust(sender, instance=kwargs['instance'])

@receiver(pre_save, sender=models.ODSTask)
def save_odstask_version(sender, **kwargs):
    to_save_version.send_robust(sender, instance=kwargs['instance'])

@receiver(pre_save, sender=models.ODSTable)
def save_odstable_version(sender, **kwargs):
    to_save_version.send_robust(sender, instance=kwargs['instance'])
