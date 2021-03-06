import pandas as pd
import pymysql
import pymysql.cursors
import datetime


def fetch_data(sql):
    
    #Connect to the database
    
    connection = pymysql.connect(host='',
                                 user='',
                                 password='',
                                 db='',
                                 charset='',
                                 port=,
                                 cursorclass=pymysql.cursors.DictCursor,
                                 connect_timeout=86400)

    try:
        with connection.cursor() as cursor:
            # Read a single record
            starttime = datetime.datetime.now() 
            print(sql + ' is running')
            cursor.execute(sql)
            result = cursor.fetchall()
            result = pd.DataFrame(result)
            
    except ValueError:
        print("Error: Error is detected ！ Simmer down, drink coffee")
        return None

    finally:
        connection.close()
        endtime = datetime.datetime.now()
        print(sql + 'is obtained')
        print('Running time: %s Seconds'%(endtime-starttime))
        
    return result


output = []
for i in range(5,59):
    sql = 'select t.jgmc, t.jyfw, y.flmc from t_jg t left join jg_ytfl y on t.jgmc = y.jgmc where ytfl = {} limit 20'.format(i)
    result = fetch_data(sql)
    output.append(result)



from functools import reduce 
a =reduce(lambda x,y: pd.concat([x,y]),output)

