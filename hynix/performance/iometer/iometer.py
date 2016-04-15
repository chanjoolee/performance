'''
Created on 2016. 3. 15.

@author: lee chanjoo
'''

import csv, itertools, cx_Oracle,datetime, subprocess, os, shutil
from _sqlite3 import Row
from itertools import product
matrix = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''

csvFiles = subprocess.check_output(["dir", "/b", "Z:\CsvDump\*.csv"], shell=True)
files = ['D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/Latency.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/Max Throughput.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/Mixed_Ran_RW.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/Mixed_Seq_RW.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/QD 1 to 32.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_Latency.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_Max Throughput.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_Mixed_Ran_RW.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_Mixed_Seq_RW.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_QD 1 to 32.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_Workload_Align.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/SLC_Workload_Unalign.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/Workload_Align.csv'
         ,'D:/hynix/requirement/Performance/Performance_raw data/Iometer_raw data/Workload_Unalign.csv'
          ]
# 01.Max Throughput.csv
for file in files:
    f = open(file, 'r')
    
#      d_reader = csv.DictReader(f)
#      headers = d_reader.fieldnames
#      for line in d_reader:
#          matrix.append(line)
    
    csvReader = csv.reader(f)
    header = []
    for row in csvReader:
        if row[0] == "'Target Type" and len(header) == 0 :
            header = row
        if row[0] ==  "WORKER" :
            dictionary = dict(zip(header, row))
            # delete ' charater
            dictionary["Target Type"] = dictionary["'Target Type"]
            del dictionary["'Target Type"]
            dictionary["meta"] = {}
            dictionary["meta"]["fileName"] = file.split('/')[len(file.split('/'))-1] #"Max Throughput.csv"
            dictionary["meta"]["measureDt"] = vToday
            dictionary["meta"]["tool"] = "iometer"
            dictionary["meta"]["product"] = "Peal Plus"
            dictionary["meta"]["nandType"] = "S36"
            dictionary["meta"]["tech"] = "SLC"
            
            dictionary["keys"] = {}
            dictionary["keys"]["Access Specification Name"] = dictionary["Access Specification Name"]
            del dictionary["Access Specification Name"]
            dictionary["keys"]["Queue Depth"] = dictionary["Queue Depth"]
            del dictionary["Queue Depth"]
            
            matrix.append(dictionary)
#             print dictionary
      
    f.close()
    
# print len(matrix)
try:
    vProduct = '' 
    con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()
    i = 0
    
    query = "delete from PERFORMANCE where " 
    query += " MEASURE_DT='" + vToday +"'"
    cur.execute(query)
    
    for m in matrix:
        i = i+1
        print i
        for mKey in m: 
            if type(m[mKey]) is dict:
                continue
#             query = "select count(*) cnt from PERFORMANCE where " 
#             query += " MEASURE_DT= '" + m["meta"]['measureDt'] +"'"
#             query += " and tool= '" + m["meta"]['tool'] +"'"
#             query += " and product = '" + m["meta"]['product'] +"'"
#             query += " and nand_type= '" + m["meta"]['nandType'] +"'"
#             query += " and tech = '" + m["meta"]['tech'] +"'"
#             query += " and data_src = '" + m["meta"]['fileName'] +"'"
#             query += " and field = '"+ mKey + "'"
#             cur.execute(query)
#             
#             cnt = cur.fetchone()[0]
#             if cnt > 0:
#                 query = "delete from PERFORMANCE where " 
#                 query += " MEASURE_DT='" + m["meta"]['measureDt'] +"'"
#                 query += " and tool= '" + m["meta"]['tool'] +"'"
#                 query += " and product = '" + m["meta"]['product'] +"'"
#                 query += " and nand_type= '" + m["meta"]['nandType'] +"'"
#                 query += " and tech = '" + m["meta"]['tech'] +"'"
#                 query += " and data_src = '" + m["meta"]['fileName'] +"'"
#                 query += " and field = '"+ mKey + "'"
#                 # cur.execute(query)
                
            query = " insert into PERFORMANCE(MEASURE_DT, TOOL, PRODUCT, NAND_TYPE, TECH, DATA_SRC, SPEC, SPEC_LABEL,QUEUE_DEPTH, FIELD, MEASURE)"
            query += " values('" + m["meta"]['measureDt'] +"','" + m["meta"]['tool'] +"','" + m["meta"]['product'] +"', '" + m["meta"]['nandType'] +"', '" + m["meta"]['tech'] +"', '" + m["meta"]['fileName'] +"',  '" + m["keys"]["Access Specification Name"] +"', '', '" + m["keys"]["Queue Depth"] +"', '"+ mKey +"',  '" + m[mKey] + "'  )"
            
            cur.execute(query)
        
    con.commit()
    
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