import redis
import re

redis_host = "localhost"
redis_port = 6379
redis_password = ""


def hello_redis():
    print("Welcome DB with NoSQL")
    while True:
        try:
                query = raw_input("Program Query Input > ")
                #print(array)
                # Basic functions (Let's get 60%!!!!!!!)
                # CREATE TABLE (table name)
                if (query.split(" ")[0].upper() == "CREATE"):
                    create(query)
                elif (query.split(" ")[0].upper() == "INSERT"):
                    insert(query)
                elif (query.split(" ")[0].upper() == "SELECT"):
                    select(query)
                elif (query.split(" ")[0].upper() == "UPDATE"):
                    update(query)
                elif (query.split(" ")[0].upper() == "DELETE"):
                    delete(query)
                elif (query.split(" ")[0].upper() == "SHOW" and query.split(" ")[1].upper() == "TABLES"):
                    showTables()
                else :
                    raise NotImplementedError
        except Exception as e:
            print(e)

def create(query):
    # output : tableName, source (att1, val1, att2, val2...)
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

def insert(query):
    # output : tableName, source (att1, val1, att2, val2...)
    # Check if the first word is 'create' and the second is 'table'
    query = query.replace(';', '')
    array = query.split()
    if (array[0].lower() != 'insert') or (array[1].lower() != 'into'):
        raise NotImplementedError
    else:
        tableName = array[2]
        query = query.split('(')[1]
        query = query.replace(')','')
        query = query.replace(' ','')
        source = query.split(',')
        print(tableName)
        print(source)

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
