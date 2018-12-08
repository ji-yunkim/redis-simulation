import redis

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
                elif (query.split(" ")[0].upper() == "SHOW"):
                    show(query)
                else :
                    raise NotImplementedError
        except Exception as e:
            print(e)

def create(query):
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
    query = query.replace(';', '')
    #query = query.replace('(','')
    #query = query.replace(')','')
    array = query.split( )
    column = array[1]
    tableName = array[3]
    subquery = ''
    print(column)
    print(tableName)
    print(array[5:])
    for i in array[5:]:
        subquery = subquery+i
    print(subquery)

def update(query):

    print(query)

def delete(query):
    print(query)

def show(query):
    print(query)

if __name__ == '__main__':
    hello_redis()
