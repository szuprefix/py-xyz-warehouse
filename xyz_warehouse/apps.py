from __future__ import unicode_literals

from django.apps import AppConfig


class Config(AppConfig):
    name = 'warehouse'

    def ready(self):
        from . import receivers
