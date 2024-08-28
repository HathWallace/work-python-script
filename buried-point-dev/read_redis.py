import csv
import time
from datetime import date, datetime

import redis
import re


# start_time = date(2024, 7, 16)
start_time = date.min
end_time = date(2024, 7, 17)
# end_time = date.max
decode_type = 'utf-8'
r = redis.StrictRedis(host='10.0.10.168', port=6379, db=0, password='@hF8sbE3BV')
day_csv = open(time.strftime('output/data_svc_%Y%m%d%H%M_day_level.csv'), 'w', newline='')
week_csv = open(time.strftime('output/data_svc_%Y%m%d%H%M_week_level.csv'), 'w', newline='')
day_csv_writer = csv.writer(day_csv, delimiter=',')
week_csv_csv_writer = csv.writer(week_csv, delimiter=',')
day_csv_writer.writerow(['event_name', 'stats_date', 'stats_value'])
week_csv_csv_writer.writerow(['event_name', 'stats_start_date', 'stats_value'])


def filter_time(redis_key_time, time_format) -> bool:
    return end_time >= datetime.strptime(redis_key_time, time_format).date() >= start_time


def record_user_login(zx_key, re_find_res):
    redis_key_time = re_find_res[0][3]
    if not filter_time(redis_key_time, '%Y%m%d'):
        return
    if re_find_res[0][1] == 'login':
        login_size = r.hlen(zx_key)
        event_name = f'{re_find_res[0][0]}_login_distinct_total'
        day_csv_writer.writerow([event_name, redis_key_time, login_size])
    else:
        total_size = int(r.get(zx_key).decode())
        event_name = f'{re_find_res[0][0]}_{re_find_res[0][1]}_total'
        day_csv_writer.writerow([event_name, redis_key_time, total_size])


def record_week_level_value_record(zx_key, re_find_res):
    stats_start_date = f'2024.{re_find_res[0][1]}'
    if not filter_time(stats_start_date, '%Y.%m.%d'):
        return
    zx_val = int(r.get(zx_key).decode())
    week_csv_csv_writer.writerow([re_find_res[0][0], stats_start_date, zx_val])


def record_week_level_hash_record(zx_key, re_find_res):
    stats_start_date = f'2024.{re_find_res[0][1]}'
    if not filter_time(stats_start_date, '%Y.%m.%d'):
        return
    for item_key_bytes, item_val_bytes in r.hgetall(zx_key).items():
        item_key = item_key_bytes.decode(decode_type)
        item_val = int(item_val_bytes.decode())
        event_name = f'{re_find_res[0][0]}_{item_key}'
        week_csv_csv_writer.writerow([event_name, stats_start_date, item_val])


def record_day_level_hash_record(zx_key, re_find_res):
    if not filter_time(re_find_res[0][1], '%Y%m%d'):
        return
    for item_key_bytes, item_val_bytes in r.hgetall(zx_key).items():
        item_key = item_key_bytes.decode(decode_type)
        item_val = int(item_val_bytes.decode())
        event_name = f'{re_find_res[0][0]}_{item_key}'
        day_csv_writer.writerow([event_name, re_find_res[0][1], item_val])


def record_page_view(zx_key, re_find_res):
    if not filter_time(re_find_res[0][1], '%Y%m%d'):
        return
    zx_val = int(r.get(zx_key).decode())
    day_csv_writer.writerow(['event_page_view', re_find_res[0][1], zx_val])



ignored_zx_key_list = [
    re.compile(r"zx_buried:enter_homepage.*"),
    re.compile(r"zx_buried:.*statistics.*"),
    re.compile(r"zx_buried:tag_\d{8}"),
    re.compile(r"zx_buried:.*record_lock"),
    re.compile(r"zx_buried:credit&borrow_stats_\d{8}"),
]


pattern_func_list = [
    (re.compile(r"zx_buried:(company|bank|gov)_(login|platform_(pc|h5))_(\d{8})"),
     record_user_login),
    (re.compile(r"zx_buried:(zone_click|kfd_loan|ad_click)_(\d{2}\.\d{2})~(\d{2}\.\d{2})"),
     record_week_level_hash_record),
    (re.compile(r"zx_buried:(credit_search|borrow_money)_(\d{2}\.\d{2})~(\d{2}\.\d{2})"),
     record_week_level_value_record),
    (re.compile(r"zx_buried:(policy_loan)_(\d{8})"),
     record_day_level_hash_record),
    (re.compile(r"zx_buried:(event_pageview)_(\d{8})"),
     record_page_view),
]


zx_keys = r.keys(pattern="zx_buried:*")
for zx_key_bytes in zx_keys:
    zx_key_str = zx_key_bytes.decode(decode_type)
    if any(len(p.findall(zx_key_str)) > 0 for p in ignored_zx_key_list):
        continue

    is_handle = False
    for p, f in pattern_func_list:
        res = p.findall(zx_key_str)
        if len(res) == 0:
            continue
        f(zx_key_str, res)
        is_handle = True
        break
    if not is_handle:
        print(f"未处理{zx_key_str}")


day_csv.close()
week_csv.close()

