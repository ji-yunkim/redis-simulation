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
    pattern0 = 'table '
    query = query.replace(';', '')
    t = re.compile(pattern0, re.IGNORECASE)
    matchTable = t.search(query)
    realQuery = query[matchTable.end():]
    tableName = realQuery.split('(')[0]
    tableName = tableName.replace(' ','')
    openingParen = realQuery.find('(')
    realQuery = realQuery[openingParen+1:-1]
    columnList = realQuery.split(',')
    source =[]
    for i in columnList:
        temp = i.split( )
        for j in temp:
            source.append(j)
    print(tableName)
    print(source)


def insert(query):
    # output : tableName, source (att1, val1, att2, val2...)
    # Check if the first word is 'create' and the second is 'table'
    query = query.replace(';', '')
    pattern0 = 'insert into '
    pattern1 = ' values'
    i = re.compile(pattern0, re.IGNORECASE)
    v = re.compile(pattern1, re.IGNORECASE)
    matchInsertInto = i.search(query)
    matchValues = v.search(query)
    tableName = query[matchInsertInto.end():matchValues.start()]
    rawSource = query[matchValues.end():]
    halfRawQuery = rawSource.split('(')[1]
    halfRawQuery = halfRawQuery.split(')')[0]
    source = []
    exec("source=["+halfRawQuery+"]")
    print(tableName)
    print(source)

def select(query):
    # output : selecting column, tableName, subquery for where clause, pattern for like matching
    query = query.replace(';', '')
    pattern0 = 'select '
    pattern1 = 'from '
    pattern2 = 'where '
    pattern3 = 'like '
    s = re.compile(pattern0, re.IGNORECASE)
    f = re.compile(pattern1, re.IGNORECASE)
    w = re.compile(pattern2, re.IGNORECASE)
    l = re.compile(pattern3, re.IGNORECASE)
    matchSelect = s.search(query)
    matchFrom = f.search(query)
    matchWhere = w.search(query)
    matchLike = l.search(query)
    column = query[matchSelect.end():matchFrom.start()]
    print(column)
    if matchWhere is not None:
        tableName = query[matchFrom.end():matchWhere.start()].split(' ')[0]
        if matchLike is not None:
            whereQuery = query[matchWhere.end():matchLike.start()]
            likeQuery = query[matchLike.end():]
            print(whereQuery)
            print(likeQuery)
        else:
            # there's no 'like'.
            whereQuery = query[matchWhere.end():]
            print(whereQuery)
    else:
        tableName = query[matchFrom.end():]
        print(tableName)



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
