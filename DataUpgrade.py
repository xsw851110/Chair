import urllib.request as request
from bs4 import BeautifulSoup as bs
import re
import pymysql
import csv
import pandas as pd

def csv2mysql(db_name, table_name, df):
    conn.select_db(db_name)
    num_new_question = len(df)
    if(num_new_question < 60):
        num_new_question = num_new_question+1
        cursor.execute('DELETE FROM '+table_name+' WHERE no < '+str(num_new_question))
    else:
        cursor.execute('TRUNCATE TABLE '+table_name)
    conn.commit()
    
    values = df.values.tolist()
    
    s = ','.join(['%s' for _ in range(len(df.columns))])
    
    cursor.executemany('INSERT INTO {} VALUES ({})'.format(table_name,s), values)

hasUpdate = False

f = open('count.txt', 'r+')
count = f.read()


url = 'https://data.gov.tw/dataset/41559'
response = request.urlopen(url);
soup = bs(response, 'html.parser')

page = soup.findAll("a", href=re.compile("^https://ws.moe.edu.tw/"))

csvlist = []
csvCount = 0
for a in page:
    csvlist.append(a.get('href'))
    csvCount = csvCount+1


newCount = csvCount/2
if(int(count) < newCount):
    f.close()
    f = open('count.txt', 'w+')
    f.write(str(int(count)+1))
    filename = count
    extension = '.csv'
    local_path = 'C:/Users/宗漢/Desktop/'+filename+extension
    request.urlretrieve(csvlist[int(count)*2], local_path)
    hasUpdate = True


df = pd.read_csv(count+'.csv', usecols=[0, 2, 3, 4, 5, 6, 7])

config = dict(host = 'localhost', user = 'PU_410403424', password = 'PU_410403424', cursorclass = pymysql.cursors.DictCursor)

conn = pymysql.Connect(**config)

conn.autocommit(1)

cursor = conn.cursor()

if(hasUpdate):
	csv2mysql('chair', 'question' , df)

conn.close()

f.close()



