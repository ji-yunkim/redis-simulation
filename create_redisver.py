
import redis
import os
import re
import errno
import tempfile
from time import time
try:
    import cPickle as pickle
except ImportError:  # pragma: no cover
    import pickle

r=redis.Redis(host='localhost',port=6379,password=None, db=0)

def hello_redis():
    print("Welcome DB with NoSQL")
    while True:
        try:
                query = raw_input("Program Query Input > ")
                #print(array)
                # Basic functions (Let's get 60%!!!!!!!)
                # CREATE TABLE (table name)
                if (query.split(" ")[0].upper() == "CREATE"):
                    create(r, query)
                elif (query.split(" ")[0].upper() == "INSERT"):
                    insert(r, query)
                # redis done
                elif (query.split(" ")[0].upper() == "SELECT"):
                    select(query)
                elif (query.split(" ")[0].upper() == "UPDATE"):
                    update(query)
                elif (query.split(" ")[0].upper() == "DELETE"):
                    delete(query)
                elif (query.split(" ")[0].upper() == "SHOW"):
                    show(query)
                else :
                    raise NotImplementedError
        except Exception as e:
            print(e)

def create(r,query):
    # Check if the first word is 'create' and the second is 'table'
    query = query.replace(';','')
    array = query.split()
    if (array[0].lower() != 'create') or (array[1].lower() != 'table'):
        raise NotImplementedError
    else:
        rawSource = array[2:]
        noEmpty = []
        source = []
        for i in rawSource:
            i = i.replace(' ','')
            noEmpty.append(i)
        if '(' not in noEmpty[0]:
            tableName = noEmpty[0]
        for i in noEmpty:
            if '(' in i:
                if i.split('(')[0] != '':
                    tableName = i.split('(')[0]
                i = i.split('(')[1]
            if ')' in i:
                i = i.replace(')','')
            if ',' in i:
                i = i.replace(',','')
            if i !='' :
                source.append(i)
        print(tableName)
        print(source)

        #col1, colT1, ...
        strIn="".join("r.hmset (\""+tableName+"\", {")
        cnt=1;
        for i in range(1, len(source)):
            if i%2 ==1:
                strIn=strIn+" \"col"+str(cnt)+"\":\""+str(source[i])+"\","
            else:
                strIn=strIn+" \"colT"+str(cnt)+"\":\""+str(source[i])+"\""
                if (i<len(source)):
                    strIn=strIn+","
                cnt=cnt+1

        strIn=strIn+"})"
        print (strIn)
        exec(strIn)
        #add to set tables (add "TABLE:")
        tableName="TABLE:"+tableName
        r.sadd("tables", tableName)



def insert(r,query):
    # Check if the first word is 'create' and the second is 'table'
    query = query.replace(';', '')
    array = query.split()
    if (array[0].lower() != 'insert') or (array[1].lower() != 'into'):
        raise NotImplementedError
    else:
        tableName = ''.join((array[2]))
        query = query.split('(')[1]
        query = query.replace(')','')
        query = query.replace(' ','')
        source = query.split(',')
        print(tableName)
        print(source)

        #col1, colT1, ...
        strIn="".join("r.hmset (\""+tableName+"\", {")
        cnt=1;
        for i in range(len(source)):
            strIn=strIn+" \"col"+str(cnt)+"\":\""+str(source[i])+"\""
            cnt=cnt+1
            if (i+1<len(source)):
                strIn=strIn+","
            elif (i+1==(len(source))):
                strIn=strIn+"})"

        print (strIn)
        exec(strIn)

        # add to set tables (add "TABLE:")
        s1=''.join(source[0])
        s1="\""+s1+"\""
        tableName="\"SET:"+tableName+"\""
        saddcomm= "r.sadd("+tableName+", "+s1+")"
        print saddcomm
        exec(saddcomm)

def select(query):
    # output : selecting column, tableName, subquery for where clause, pattern for like matching
    query = query.replace(';', '')
    # it does not fliter '(' and ')'. they will be handled separately. To filter them, user the codes below.
    # query = query.replace('(','')
    # query = query.replace(')','')
    array = query.split()
    column = array[1]
    tableName = array[3]
    print(column)
    print(tableName)
    # if it consists only 'select' and 'from', it does not make 'subquery' and 'pattern' variables.
    if len(array) > 4:
        subquery = ''
        pattern = array[-1]
        for i in array[5:-2]:
            subquery = subquery + i
        print(subquery)
        print(pattern)

def update(query):
    # output : tableName, subquery for set, subquery for where
    query = query.replace(';', '')
    # it does not filter '(' and ')'. they will be handled separately. To filter them, user the codes below.
    # query = query.replace('(','')
    # query = query.replace(')','')
    array = query.split()
    tableName = array[1]
    print(tableName)
    pattern1 = 'set '
    pattern2 = 'where '
    s = re.compile(pattern1, re.IGNORECASE)
    w = re.compile(pattern2, re.IGNORECASE)
    matchSet = s.search(query)
    matchWhere = w.search(query)
    if matchWhere is not None:
        setQuery = query[matchSet.end():matchWhere.start()]
        whereQuery = query[matchWhere.end():]
        print(setQuery)
        print(whereQuery)
    else:
        setQuery = query[matchSet.end():]
        print(setQuery)

def delete(query):
    # output : tableName, subquery for set, subquery for where
    query = query.replace(';', '')
    # it does not filter '(' and ')'. they will be handled separately. To filter them, user the codes below.
    # query = query.replace('(','')
    # query = query.replace(')','')
    array = query.split()
    tableName = array[2]
    print(tableName)
    pattern2 = 'where '
    w = re.compile(pattern2, re.IGNORECASE)
    matchWhere = w.search(query)
    if matchWhere is not None:
        whereQuery = query[matchWhere.end():]
        print(whereQuery)

def showTables():
    # blank!
    print()

if __name__ == '__main__':
    hello_redis()