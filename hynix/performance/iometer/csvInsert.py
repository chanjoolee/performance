'''
Created on 2016. 3. 15.

@author: lee chanjoo
'''

import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import csv, itertools, cx_Oracle,datetime, os, re , math , shutil
from _sqlite3 import Row
from itertools import product
from telnetlib import theNULL
from test.test_datetime import DSTEND



#Iometer start
matrix = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''
gFolderInfos = []
filesIometer = []

f = open('D:/hynix/requirement/Etc/jiraResolution.csv', 'r')
 
#      d_reader = csv.DictReader(f)
#      headers = d_reader.fieldnames
#      for line in d_reader:
#          matrix.append(line)
 
#folderInfo
csvReader = csv.reader(f)
header = ['ID','RESOLUTIONDATE']
 
for row in csvReader:
    if row[1] == "RESOLUTIONDATE":
        continue
    else :
        dictionary = dict(zip(header, row))
        matrix.append(dictionary)
#             print dictionary
   
f.close()
     
# print len(matrix)
try:
     
    vProduct = '' 
#     con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
    con_str = 'jirauser/JiraPass12@166.125.19.98:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()
     
    i = 0
    for m in matrix:
        i = i+1
        print repr(i) + '(' + repr(len(matrix)) + ')'
        query = " insert into JIRAISSUE_TMP(ID, RESOLUTIONDATE)"
        query += " values('" + m['ID'] +"',to_date('" + m['RESOLUTIONDATE'] +"','YYYY/MM/DD hh24:mi:ss') )"
         
        cur.execute(query)
        con.commit()
    #Iometer End
    
    
except cx_Oracle.DatabaseError ,ex:
    error, = ex.args    
    print query
    print 'Error Inserting Field Base'
    print 'Error.code    =', error.code
    print 'Error.message =', error.message
    print 'Error.offset  =', error.offset
    con.rollback()
    
finally :
    if cur:
        cur.close()     
    if con:
        con.close()    