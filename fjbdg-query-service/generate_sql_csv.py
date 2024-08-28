import time

import pandas as pd

excel_path = r"D:\1File\20240812_个体工商户改造\模型输出加工开发v2.xlsx"
df = pd.read_excel(excel_path)
tbl_info_df = pd.read_excel(excel_path, sheet_name='涉及数据表')
filtered_df = df[df['实际二期数据字段'] != '?']
filtered_tbl_info_df = tbl_info_df[tbl_info_df['法人授权关键字'] != '?']

key_dict = {}
for idx, row in filtered_tbl_info_df.iterrows():
    table_name = row['二期表名']
    search_key = row['法人授权关键字']
    key_dict[table_name] = search_key


sql_dict = {}
for idx, row in filtered_df.iterrows():
    table_name = row['二期表名']
    field_name = row['实际二期数据字段']
    if field_name == '?':
        continue
    if table_name not in sql_dict:
        sql_dict[table_name] = []
    sql_dict[table_name].append(field_name)

output_data = {'tbl_name': [], 'exe_sql': []}
for table_name, field_name_list in sql_dict.items():
    if table_name in key_dict:
        sql_str = f"SELECT {', '.join(field_name_list)} FROM {table_name} WHERE {key_dict[table_name]} = :mainCode"
    else:
        continue
    output_data['tbl_name'].append(table_name)
    output_data['exe_sql'].append(sql_str)

output_excel_path = time.strftime('../output/%Y%m%d%H%M.csv')
output_df = pd.DataFrame(output_data)
output_df.to_csv(output_excel_path, index=False)
