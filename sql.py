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


TABLE_INFO = tablesInfo('files/metadata.txt')
TABLE_DATA = defaultdict()
    

def QuerySolver(query):
    # validating the query
    if query[0].lower() != 'select':
        print("Wrong query!!!")
        return -1
    if query[1].lower() == 'distinct':
        if query[3].lower() != 'from':
            print(1)
            print("Wrong query!!!")
            return -1
    else:
        if query[2].lower() != 'from':
            print(query[2])
            print("Wrong query!!!")
            return -1
    if query[1] == '*':
        table_name = query[3]
        if not TABLE_INFO[table_name]:
            print('Wrong table name')
            return -1
        printlabel = TABLE_INFO[table_name]
        printtable = TABLE_DATA[table_name]
        printdata(printlabel,printtable)
 
def printdata(tablelabel,tabledata):
    # for i in tablelabel:
    print(tablelabel)
    for i in tabledata:
        print(tuple(i))



if __name__ == "__main__":
    for name in TABLE_INFO:
        # print(name)
        TABLE_DATA[name] = tablesData(name)                
        # print(TABLE_DATA[name])
    
    try:
        query = sys.argv[1]
        query = query.split(';')[0]
        query = query.split(' ')
        # print(query)
        QuerySolver(query)
    except:
        print("Wrong query")