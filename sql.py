import sqlparse
import csv
import sys
from collections import defaultdict

AGGS = ['sum(','avg(','max(','min(']

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
                    col_names.append(name+'.'+meta_lines[i].split('\n')[0])
                    i += 1
                table_info[name] = col_names
    except:
        print("Error Occured while collecting Table Info")
    return table_info
            

def tablesData(tablename):
    table_data = []
    try:
        data = open('files/' + tablename + '.csv','r')
        rows = csv.reader(data)
        for row in rows:
            l = []
            for col in row:
                l.append(int(col))
            table_data.append(l)
    except:
        print("Error occured while collecting",tablename, "data")
    return table_data


TABLE_INFO = tablesInfo('files/metadata.txt')
TABLE_DATA = defaultdict()

def joinTables(tab1,row1,tab2,row2):
    final_table = []
    for t1 in tab1:
        for t2 in tab2:
            final_table.append(t1+t2)
    
    final_row = row1+row2
    return final_table,final_row


def QuerySolver(query):
    if IsQueryValid(query):
        i = 0
        while query[i].lower() != 'from':
            i += 1
        tables = query[i+1].split(',')
        
        final_table,final_row = TABLE_DATA[tables[0]], TABLE_INFO[tables[0]]
        for j in range(1,len(tables)):
            final_table,final_row = joinTables(final_table,final_row,TABLE_DATA[tables[j]], TABLE_INFO[tables[j]])
        
        attr = query[1].split(',')
        astrick = 0
        aggs = 0
        projection = 0
        end_rows = []
        for ats in attr:
            if ats == '*':
                printdata(final_row,final_table)
                return 1
            else:
                for func in AGGS:
                    if func in ats:
                        aggs += 1
              
                for j in final_row:
                    if '.' in ats:
                        if ats == j:
                            end_rows.append(ats)
                            break
                    else:
                        for name in TABLE_INFO:
                            p = name + '.' + ats
                            if p == j:
                                end_rows.append(p)
                                break
                        else:
                            continue
                        break

        if aggs:
            if aggs != len(query):
                return -1
            # else:

        else:
            end_tabs = []
            for i in range(len(final_table)):
                a_row = []
                for row in end_rows:
                    for r in range(len(final_row)):
                        if row == final_row[r]:
                            a_row.append(final_table[i][r])
                            break
                end_tabs.append(a_row)
            printdata(end_rows,end_tabs) 

    else:
        return -1    
 

def IsQueryValid(query):
    if query[0].lower() != 'select':
        return False
    if query[1].lower() == 'distinct':
        if query[3].lower() != 'from':
            return False
    else:
        if query[2].lower() != 'from':
            return False
    return True

def printdata(tablelabel,tabledata):
    print(','.join(map(str,tablelabel))) 
    for i in tabledata:
       print(','.join(map(str,i))) 



if __name__ == "__main__":
    for name in TABLE_INFO:
        TABLE_DATA[name] = tablesData(name)                
    
    query = ((sys.argv[1]).split(';')[0]).split(' ')
    QuerySolver(query)