import json
import math
import sys
import time

import pandas as pd

excel_path = r"D:\1File\20240812_个体工商户改造\模型输出加工开发v2.xlsx"
output_json_path = time.strftime('../output/sql_query_masking_rules.%Y%m%d%H%M.json')

df = pd.read_excel(excel_path)
rule_df = pd.read_excel(excel_path, sheet_name='_通用加工类型')
table_name_col = '二期表名'
table_name_cn_col = '实际二期数据表'
field_name_col = '实际二期数据字段'
field_name_cn_col = '实际二期数据字段描述'
rule_index_col = '加工类型索引'
rule_para_col = '参数值'

common_rule_dict = {}
filtered_rule_df = rule_df[rule_df[rule_index_col] != '']
for idx, row in filtered_rule_df.iterrows():
    rule_idx = row[rule_index_col]
    print(rule_idx)
    if '{N}' in rule_idx:
        common_rule_dict[rule_idx] = 'need_para'
    else:
        common_rule_dict[rule_idx] = ''

filtered_df = df[df[field_name_col] != '?']
table_rule_dict = {}
for idx, row in filtered_df.iterrows():
    table_name = row[table_name_col]
    field_name = row[field_name_col]
    rule_idx = row[rule_index_col]
    if table_name not in table_rule_dict:
        table_rule_dict[table_name] = {
            '_note': row[table_name_cn_col]
        }
    if field_name in table_rule_dict[table_name]:
        print(f'error: 字段重复：表{table_name}的字段{field_name}')
        sys.exit(1)
    if rule_idx not in common_rule_dict:
        print(f'warning: 加工类型索引无效：表{table_name}的字段{field_name}')
        continue
    rule_info = common_rule_dict[rule_idx]
    rule_para = row[rule_para_col]
    field_rule_info = {
        '_note': row[field_name_cn_col],
        'rule': rule_idx
    }
    if rule_info == '':
        if not math.isnan(rule_para):
            print(f'warning: 加工参数无效：表{table_name}的字段{field_name}')
    elif rule_info == 'need_para':
        if math.isnan(rule_para):
            print(f'warning: 加工参数应有但未有：表{table_name}的字段{field_name}')
            continue
        field_rule_info['n'] = int(rule_para)
    else:
        print(f'warning: rule_info无效')
        continue
    table_rule_dict[table_name][field_name] = field_rule_info

with open(output_json_path, 'w', encoding='utf-8') as json_file:
    json.dump(table_rule_dict, json_file, ensure_ascii=False, indent=2)
