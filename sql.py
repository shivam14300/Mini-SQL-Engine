import sqlparse
import csv
import sys
from collections import defaultdict


def tablesInfo(filename):
    table_info = defaultdict(list)
    try:
        meta = open(filename,'r')
        meta_lines = meta.readlines()
        # print(meta_lines)
        for i in range(len(meta_lines)):
            if "<begin_table>" in meta_lines[i]:
                # print(i)
                i += 1
                name = meta_lines[i].split('\n')[0]
                i += 1
                col_names = []
                while "end_table" not in meta_lines[i]:
                    col_names.append(meta_lines[i].split('\n')[0])
                    i += 1
                table_info[name] = col_names
    except:
        print("Error Occured while collecting Table Info")
        pass
    return table_info
            

def tablesData(tablename):
    table_data = []
    try:
        data = open('files/' + tablename + '.csv','r')
        data_lines = data.readlines()
        # print(data_lines)
        for i in range(len(data_lines)):
            x = data_lines[i].split('\n')[0]
            table_data.append(x.split(','))
    except:
        print("Error occured while collecting ",tablename, "data")
    return table_data


if __name__ == "__main__":
    TABLE_INFO = tablesInfo('files/metadata.txt')
    # print(TABLE_INFO)
    TABLE_DATA = defaultdict()
    for name in TABLE_INFO:
        # print(name)
        TABLE_DATA[name] = tablesData(name)                
        # print(TABLE_DATA[name])