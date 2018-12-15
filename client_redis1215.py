
import redis
#from pyrediscluster import StrictRedisCluster
import re

'''
checkpoint 
1. select and table structure (very top row) -> * 
2. selection when "where" is in place (and like) 
3. update after delete is complete
4. delete when given value (not key)
5. delete hash value as well as set index
'''

r = redis.StrictRedis(host='localhost',port=6379,charset="utf-8", password=None, db=0, decode_responses=True)

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
                    show()
                elif (query.split(" ")[0].upper() == "FLUSHALL"):
                    flush()
                else :
                    raise NotImplementedError
        except Exception as e:
             print(e)

def create(r,query):
    # Check if the first word is 'create' and the second is 'table'

    # ******* Parsing part *******
    # output : tableName, source (att1, val1, att2, val2...)
    # Check if the first word is 'create' and the second is 'table'
    pattern0 = 'table '
    query = query.replace(';', '')
    t = re.compile(pattern0, re.IGNORECASE)
    matchTable = t.search(query)
    realQuery = query[matchTable.end():]
    tableName = realQuery.split('(')[0]
    tableName = tableName.replace(' ', '')
    openingParen = realQuery.find('(')
    realQuery = realQuery[openingParen + 1:-1]
    columnList = realQuery.split(',')
    source = []
    for i in columnList:
        temp = i.split()
        for j in temp:
            source.append(j)

    # ******* Redis part *******
    strIn = "".join("r.hmset (\"" + "META:"+tableName + "\", {")
    for i in range(0, len(source)):
        if i % 2 == 0:
            # column name saving
            strIn = strIn + " \"" + str(source[i]) + "\":\"" + str(source[i+1]) + "\","
    strIn = strIn + "\"size\":0})"
    strln = strIn + "})"
    #r.hmset ("META:T1", { "make":"int", "model":"int","size":0})
    #print (strIn)
    exec (strIn)
    # add to set tables (add "TABLE:")
    tableName = "TABLE:" + tableName
    r.sadd("Tables", tableName)

def insert(r,query):
    # Check if the first word is 'create' and the second is 'table'

    # ******* Parsing part *******
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

    # ******* Redis part *******
    # T1 / (123, 458) => ['123','458'] / ('123','458') => ["'123'","'458'"]
    # Making a hash 'tableName:rowNum' (hmset)
    hashKeys = r.hkeys("META:" + tableName)
    currentSize=int(r.hget("META:"+tableName,"size"))
    rowNum = currentSize+1
    strIn = "".join("r.hmset (\"" + tableName+":" + str(rowNum) + "\", {")
    metaLen = r.hlen("META:"+tableName)
    j=0
    for i in range(0, metaLen):
        if 'size' not in hashKeys[i]:
            strIn = strIn + " \"" + str(hashKeys[i]) + "\":\"" + str(source[j]) + "\","
            saddQuery = "r.sadd("+"\""+str(hashKeys[i])+":"+ str(source[j])+"\","+str(rowNum)+")"
            exec(saddQuery)
            # r.sadd("make:123",1), r.sadd("model:458",1)
            # if type is integer
            if ("int" in r.hget("META:"+tableName, hashKeys[i])):
                # zadd(sortedSetName,{value:score})
                zaddQuery = "r.zadd(\""+tableName+":"+hashKeys[i]+"\",{\""+str(rowNum)+"\":"+str(source[j])+"})"
                exec(zaddQuery)
            # Do we sort CHAR type, also?
            j=j+1
    strln = strIn + "})"
    exec(strln)
    # r.hmset ("T1:1", { "make":"123", "model":"458",})
    # Updating metadata (size)
    updateQuery = "r.hset(\"META:"+tableName+"\",\"size\","+str(rowNum)+")"
    exec(updateQuery)
    #exec('r.hmset (\"'+tableName+':'+id+',{\"'+make+'\":'+123', '+model+':'+458+'})')

def select(query):
    # output : selecting column, tableName, subquery for where clause, pattern for like matching

    # ******* Parsing Part *******
    query = query.replace(';', '')
    # it does not fliter '(' and ')'. they will be handled separately. To filter them, user the codes below.
    # query = query.replace('(','')
    # query = query.replace(')','')
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
        tableName = query[matchFrom.end():matchWhere.start()]
        print(tableName)
        if matchLike is not None:
            whereQuery = query[matchWhere.end():matchLike.start()]
            likeQuery = query[matchLike.end():]
            print(whereQuery)
            print(likeQuery)
        else:
            whereQuery = query[matchWhere.end():]
            print(whereQuery)
    else:
        tableName = query[matchFrom.end():]
        print(tableName)

    # *******Redis Part*******
    if "," in column:
        groupList = column.split(",")

 ################## How to implement 'WHERE' inside Redis? What will be its output? ###############






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
        findK= whereQuery.split("=")
        print findK[1]
    if (r.sismember("SET:"+tableName, findK[1])):
        r.srem("SET:"+tableName, findK[1])
        #delete hash for memory efficiency
        #r.hdel(str(findK[1]))
        #method delete with key

        print ("OK")
    #else: #delete with value
        #search value
        #find key
        #delete with key (recursion?)


def show():
    #done
    tablelist=r.smembers('Tables')
    print("Tables")
    print ("-------------")
    for t in tablelist:
        newt=t.split(":")
        print newt[1]

def flush():
    r.flushall()
    print("All data flushed!")

if __name__ == '__main__':
    hello_redis()