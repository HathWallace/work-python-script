import csv
import time

file_path = "D:\\1File\\20240802_政策仿真\\仿真企业.txt"
csv_file = open(time.strftime("output\\仿真企业_整理_%Y%m%d%H%M.csv"), 'w', newline='')
csv_writer = csv.writer(csv_file, delimiter=',')
with open(file_path, 'r') as file:
    line = file.readline()
    row = [None]
    page_index = 0
    while line:
        if line == '"1\n':
            page_index += 1
            row[0] = page_index
        if line != '\t\n':
            row.append(line.strip().strip('"'))
            line = file.readline()
            if line != '\t\n':
                row_index = int(row[1].replace('"', ''))
                next_row_index = row_index + 1
                while str(next_row_index) in row:
                    idx = row.index(str(next_row_index))
                    if len(row) - idx < 5:
                        break
                    row.insert(idx, page_index)
                    csv_writer.writerow(row[:idx])
                    row = row[idx:]
                    next_row_index += 1
                csv_writer.writerow(row)
                row.clear()
                row.append(page_index)
                continue
        line = file.readline()

csv_file.close()
