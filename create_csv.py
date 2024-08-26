import csv
import time
import datetime
import random


with open(time.strftime('%Y%m%d%H%M.csv'), 'w', newline='') as csvfile:
    spamwriter = csv.writer(csvfile, delimiter=',')
    spamwriter.writerow(['event_name', 'stats_date', 'stats_value'])
    stats_date = datetime.datetime(2024,5,5)
    for i in range(60):
        h5_dist_cnt = random.randint(10, 1000)
        h5_cnt = h5_dist_cnt + random.randint(h5_dist_cnt//10, h5_dist_cnt//5)
        pc_dist_cnt = random.randint(20,200)
        pc_cnt = pc_dist_cnt + random.randint(pc_dist_cnt//10, pc_dist_cnt//5)
        stats_date += datetime.timedelta(days=1)
        spamwriter.writerow(['enter_homepage_h5_distinct_total', stats_date.strftime('%Y-%m-%d'), h5_dist_cnt])
        spamwriter.writerow(['enter_homepage_h5_total', stats_date.strftime('%Y-%m-%d'), h5_cnt])
        spamwriter.writerow(['enter_homepage_pc_distinct_total', stats_date.strftime('%Y-%m-%d'), pc_dist_cnt])
        spamwriter.writerow(['enter_homepage_pc_total', stats_date.strftime('%Y-%m-%d'), pc_cnt])
