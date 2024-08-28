import sys

from create_redis_data import write_redis_hash

data_str = sys.argv[1]
data_str_in_redis = data_str.replace('-', '')
user_type, user_cnt = sys.argv[2:4]
user_type_key = f'zx_buried:{user_type}_login_{data_str_in_redis}'
write_redis_hash(user_type_key, user_cnt)
print(f"write to {user_type_key} ok.")
