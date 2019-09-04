import sqlparse
import csv
import sys
from collections import defaultdict

AGGS = ['sum(','avg(','max(','min(']
OPS = ['<','>','=']
DOUBLE_OPS = ['<=','>=']
def tablesInfo(filename):
    table_info = defaultdict(list)
    try:
        meta = open(filename,'r')
        meta_lines = meta.readlines()
        for i in range(len(meta_lines)):
            if "<begin_table>" in meta_lines[i]:
                name = meta_lines[i+1].split('\n')[0]
                i += 2
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
    distinct = 0
    if query[1] == 'distinct':
        distinct = 1

    if IsQueryValid(query,distinct):
        i = 0
        while i<len(query) and query[i].lower() != 'from':
            i += 1
        tables = query[i+1].split(',')
        
        final_table,final_row = TABLE_DATA[tables[0]], TABLE_INFO[tables[0]]
        for j in range(1,len(tables)):
            final_table,final_row = joinTables(final_table,final_row,TABLE_DATA[tables[j]], TABLE_INFO[tables[j]])
        
        if distinct:
            attr = query[2].split(',')
        else:
            attr = query[1].split(',')
        
        where_idx = -1
        for i in range(len(query)):
            if query[i].lower() == 'where':
                where_idx = i
                break
        final_condition = []
        final_ops = []
        final_table1 = []
        final_table2 = []
        if where_idx > 0:
            final_condition, final_ops = makeConditionTable(where_idx,query)
        if len(final_condition) == 1:
            final_table,final_row = processWhere(final_table,final_row,final_condition[0])
        elif len(final_condition) == 2:
            final_table1,final_row1 = processWhere(final_table,final_row,final_condition[0])
            final_table2,final_row2 = processWhere(final_table,final_row,final_condition[1])
            if final_ops[0] == 'OR':
                final_table = final_table1+final_table2
            else:
                final_table = intersection(final_table1,final_table2)
        
        aggs = 0
        for ats in attr:
            if ats == '*':
                printdata(final_row,final_table)
                return 1
        for ats in attr:
            for func in AGGS:
                if func in ats:
                    aggs += 1
        if aggs and aggs != len(attr):
            return -1
        
        end_rows = []
        end_tabs = []
        if aggs:
            end_rows = makeEndRows(attr,final_row,1)
            end_tabs = makeEndTabs(distinct,end_rows,final_row,final_table,1)
        else:
            end_rows = makeEndRows(attr,final_row,2)
            end_tabs = makeEndTabs(distinct,end_rows,final_row,final_table,2)

        printdata(end_rows,end_tabs)

    else:
        print('invalid query')
        return -1

def intersection(lst1, lst2): 
    lst3 = [value for value in lst1 if value in lst2] 
    return lst3 

def processWhere(table,row,condition):
    f1 = 0
    f2 = 0
    for r in row:
        x = r.split('.')[1]
        if condition[0] == x or condition[0] == r:
            f1 = 1
            condition[0] = r
            break 
    for r in row:
        x = r.split('.')[1]
        if condition[2] == x or condition[2] == r:
            f2 = 1
            condition[2] = r
            break
    a = condition[0]
    b = condition[2]
    idx_a = -1
    idx_b = -1
    if f1 == 1:
        idx_a = 0
        while row[idx_a] != a:
            idx_a += 1 
    if f2 == 1:
        idx_b = 0
        while row[idx_b] != b:
            idx_b += 1 

    final_tabel = []
    if condition[1] == '<':
        if f1 == 0 and f2 == 0:
            if int(a) < int(b):
                final_tabel = table
        if f1 == 0 and f2 == 1:
            for i in range(len(table)):
                if int(a) < table[i][idx_b]:
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 0:
            for i in range(len(table)):
                if table[i][idx_a] < int(b):
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 1:
            for i in range(len(table)):
                if table[i][idx_a] < table[i][idx_b]:
                    final_tabel.append(table[i])

    if condition[1] == '>':
        if f1 == 0 and f2 == 0:
            if int(a) > int(b):
                final_tabel = table

        if f1 == 0 and f2 == 1:
            for i in range(len(table)):
                if int(a) > table[i][idx_b]:
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 0:
            for i in range(len(table)):
                if table[i][idx_a] > int(b):
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 1:
            for i in range(len(table)):
                if table[i][idx_a] > table[i][idx_b]:
                    final_tabel.append(table[i])
    
    if condition[1] == '<=':
        if f1 == 0 and f2 == 0:
            if int(a) <= int(b):
                final_tabel = table

        if f1 == 0 and f2 == 1:
            for i in range(len(table)):
                if int(a) <= table[i][idx_b]:
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 0:
            for i in range(len(table)):
                if table[i][idx_a] <= int(b):
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 1:
            for i in range(len(table)):
                if table[i][idx_a] <= table[i][idx_b]:
                    final_tabel.append(table[i])
    
    if condition[1] == '>=':
        if f1 == 0 and f2 == 0:
            if int(a) >= int(b):
                final_tabel = table

        if f1 == 0 and f2 == 1:
            for i in range(len(table)):
                if int(a) >= table[i][idx_b]:
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 0:
            for i in range(len(table)):
                if table[i][idx_a] >= int(b):
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 1:
            for i in range(len(table)):
                if table[i][idx_a] >= table[i][idx_b]:
                    final_tabel.append(table[i])

    if condition[1] == '=':
        if f1 == 0 and f2 == 0:
            if int(a) == int(b):
                final_tabel = table

        if f1 == 0 and f2 == 1:
            for i in range(len(table)):
                if int(a) == table[i][idx_b]:
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 0:
            for i in range(len(table)):
                if table[i][idx_a] == int(b):
                    final_tabel.append(table[i])

        if f1 == 1 and f2 == 1:
            for i in range(len(table)):
                if table[i][idx_a] == table[i][idx_b]:
                    final_tabel.append(table[i])
            del row[idx_b]
            for i in range(len(final_tabel)):
                del final_tabel[i][idx_b]
    return final_tabel,row

def makeConditionTable(where_idx,query):
    final_condition = []
    conditions = []
    ops = []
    a_condition = []
    for i in range(where_idx+1,len(query)):
        if query[i] == 'AND' or query[i] == 'OR':
            ops.append(query[i])
            conditions.append(a_condition)
            a_condition = []
        else:
            a_condition.append(query[i])                
    if len(a_condition) > 0:
        conditions.append(a_condition)
    
    for i in range(len(conditions)):
        new_cond = []
        for x in range(len(conditions[i])):
            f = 1
            for o in DOUBLE_OPS:
                if o in conditions[i][x]:
                    f = 0
                    t = conditions[i][x].split(o)
                    if t[0] != '':
                        new_cond.append(t[0])
                    new_cond.append(o)
                    if t[1] != '':
                        new_cond.append(t[1])
            if f:
                for o in OPS:
                    if o in conditions[i][x]:
                        f = 0
                        t = conditions[i][x].split(o)
                        if t[0] != '':
                            new_cond.append(t[0])
                        new_cond.append(o)
                        if t[1] != '':
                            new_cond.append(t[1])
            if f:
                new_cond.append(conditions[i][x])
        final_condition.append(new_cond)
    return final_condition,ops

def makeEndTabs(distinct,end_rows,final_row,final_table,flag):
    end_tabs = []
    end_tabs1 = []
    if flag == 1:
        a_row = []
        if len(final_table) > 0:
            for op in end_rows:
                x = aggregate(op,final_row,final_table)
                a_row.append(x)
            end_tabs1.append(a_row)
    else:
        for i in range(len(final_table)):
            a_row = []
            for row in end_rows:
                for r in range(len(final_row)):
                    if row == final_row[r]:
                        a_row.append(final_table[i][r])
                        break
            end_tabs1.append(a_row)  
    if distinct == 1: 
        end_tabs.append(end_tabs1[0])
        for i in range(1,len(end_tabs1)):
            f = 1
            for j in range(len(end_tabs)):
                if end_tabs1[i] == end_tabs[j]:
                    f = 0
                    break
            if f == 1:
                end_tabs.append(end_tabs1[i])
    else:
        for i in range(len(end_tabs1)):
            end_tabs.append(end_tabs1[i])
    return end_tabs

def makeEndRows(attr,final_row,flag):
    end_rows = []
    if flag == 1:
        for ats in attr:
            agg_query = (ats.split('(')[1]).split(')')[0]
            agg_func = ats.split('(')[0]
            for j in final_row:
                if '.' in agg_query:
                    if agg_query == j:
                        end_rows.append(ats)
                        break
                else:
                    for name in TABLE_INFO:
                        p = name + '.' + agg_query
                        if p == j:
                            end_rows.append(agg_func + '(' + p + ')')
                            break
                    else:
                        continue
                    break
    else:
        for ats in attr:  
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
    return end_rows

                

def aggregate(query,final_row,final_table):
    agg_func = query.split('(')[0]
    col = (query.split('(')[1]).split(')')[0]
    idx = 0
    while final_row[idx] != col:
        idx += 1
    if agg_func == 'max':
        ans = -100000000000
        for i in range(len(final_table)):
            ans = max(ans,final_table[i][idx]) 
    if agg_func == 'min':
        ans = 100000000000
        for i in range(len(final_table)):
            ans = min(ans,final_table[i][idx])
    if agg_func == 'sum':
        ans = 0
        for i in range(len(final_table)):
            ans += final_table[i][idx]
    if agg_func == 'avg':
        ans = 0
        for i in range(len(final_table)):
            ans += final_table[i][idx]
        ans = ans/len(final_table)
    return ans
def IsQueryValid(query,distinct):
    if query[0].lower() != 'select':
        return False
    if distinct:
        if query[3].lower() != 'from':
            return False
        if len(query) > 5 and query[5].lower() != 'where':
            return False
    else:
        if len(query) > 4 and query[4].lower() != 'where':
            return False        
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
    
    arg = (sys.argv[1]).replace(', ',',')
    query = (arg.split(';')[0]).split(' ')
    QuerySolver(query)