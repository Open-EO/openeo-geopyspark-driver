
from openeogeotrellis.job_registry import ZkJobRegistry
import datetime
import pandas as pd

with ZkJobRegistry() as registry:
    jobs_before = registry.get_all_jobs_before(datetime.datetime.now())
    df = pd.DataFrame(jobs_before)
    df.created = pd.to_datetime(df.created)
    df.index = df.created
    print(df.status.unique())
    df = df[(df.status == 'finished') | (df.status == 'error')| (df.status == 'canceled')]
    df = df[(df.user_id != 'jenkins') & (df.user_id != 'geopyspark-integrationtester')]

    df['yearmonth'] = df.index.strftime('%Y%m')
    df['cpuhour'] = df.cpu_time_seconds / 3600.0
    df['cpuhour'] = df.cpu_time_seconds / 3600.0
    df['cost'] = df.cpuhour * 0.01
    df['memoryhour'] = df.memory_time_megabyte_seconds / (3600 * 1024)
    df['memorycost'] = df.memoryhour * 0.005
    df['totalcost'] = df.memorycost + df.cost

    cost_by_user_month = df.groupby(['user_id', 'yearmonth']).sum().cost
    cost_by_user_month = cost_by_user_month[cost_by_user_month > 1.0]
    memorycost_by_user_month = df.groupby(['user_id', 'yearmonth']).sum().memorycost
    memorycost_by_user_month = memorycost_by_user_month[memorycost_by_user_month > 1.0]

    total_cost = (memorycost_by_user_month + cost_by_user_month).round()

    usageinfo = pd.DataFrame({'cpu': df.cpu_time_seconds, 'memory': df.memory_time_megabyte_seconds, 'yearmonth':df.yearmonth}, index=df.index)
    usageinfo['yearmonth'] = usageinfo.index.strftime('%Y%m')
    usageinfo['cpuhour'] = usageinfo.cpu / 3600.0

    usageinfo['cost'] = usageinfo.cpuhour * 0.02


    totalusagepermonth = usageinfo.groupby('yearmonth').sum()

