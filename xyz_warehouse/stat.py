from . import choices
from .models import Output
from xyz_etl.models import Source
import pandas as pd
import datetime


def create_daily_result(obj):
    max_timestamp = obj.get_max_timestamp()

    if max_timestamp == None:
        return
    else:
        start_max_timestamp = max_timestamp.strftime('%Y-%m-%d 00:00:00')
        end_max_timestamp = (max_timestamp + datetime.timedelta(days=1)).strftime('%Y-%m-%d 00:00:00')
        sync_table = obj.name
        sync_date = datetime.datetime.now()
        ods_task = obj.task.name
        id = sync_date.strftime("%Y%m%d") + '_' + ods_task + '_' + sync_table
        if obj.source_timestamp_field == '-':
            df1, df2 = obj.pair_query("count(1)")
        else:
            df1, df2 = obj.pair_query("count(1)", "where %s>='%s' and %s<'%s'" % (
            obj.the_source_timestamp_field, start_max_timestamp, obj.the_source_timestamp_field, end_max_timestamp))
        source_count = df1.loc[0][0]
        result_count = df2.loc[0][0]
        diff_count = result_count - source_count
        if source_count == 0:
            finish_rate = 0
        else:
            finish_rate = result_count / source_count * 100
        result_field = {"id": id, "ods_task": ods_task, "sync_date": sync_date, "sync_table": sync_table,
                        "source_count": source_count, "result_count": result_count, "diff_count": diff_count,
                        "finish_rate": finish_rate}
        data = pd.DataFrame([result_field])
        output, created = Output.objects.get_or_create(
            code="xqh",
            defaults=dict(
                name="daily_compare_result",
                table_name="dailyresult",
                db=Source.objects.get(name='warehouse'),
                primary_key="id",
                write_mode=choices.WRITE_MODE_DELTA_SYNC
            )
        )
        output.run(data)
