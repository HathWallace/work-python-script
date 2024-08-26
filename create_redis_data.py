import os
import redis
import sys

r = redis.StrictRedis(host='10.0.10.168', port=6379, db=0, password='@hF8sbE3BV')


def write_redis_hash(redis_key, hash_cnt):
    hash_cnt = int(float(hash_cnt))
    for i in range(hash_cnt):
        r.hset(redis_key, f'fake_user_{os.urandom(4).hex()}', 'fake_platform')
    r.expire(redis_key, 21*24*60*60)


if __name__ == '__main__':
    data_str = sys.argv[1]
    data_str_in_redis = data_str.replace('-', '')
    company_cnt, bank_cnt, gov_cnt = sys.argv[2:5]
    company_key = f'zx_buried:company_login_{data_str_in_redis}'
    bank_key = f'zx_buried:bank_login_{data_str_in_redis}'
    gov_key = f'zx_buried:gov_login_{data_str_in_redis}'
    write_redis_hash(company_key, company_cnt)
    write_redis_hash(bank_key, bank_cnt)
    write_redis_hash(gov_key, gov_cnt)
    json_str = ''
    json_str += "[\"com.jfy.analysis.entity.VisitorData\",{\"date\":\"%s\",\"type\":\"企业\",\"count\":%d}]," % (data_str, int(float(company_cnt)))
    json_str += "[\"com.jfy.analysis.entity.VisitorData\",{\"date\":\"%s\",\"type\":\"金融机构\",\"count\":%d}]," % (data_str, int(float(bank_cnt)))
    json_str += "[\"com.jfy.analysis.entity.VisitorData\",{\"date\":\"%s\",\"type\":\"政府\",\"count\":%d}]," % (data_str, int(float(gov_cnt)))
    print(json_str)
