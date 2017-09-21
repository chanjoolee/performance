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
import subprocess

#Iometer start
subprocess.call(r'net use z: /del', shell=True)
subprocess.call(r'net use z: \\10.15.202.120\das_input /user:hynixad\probe test')

matrix = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''
path = "//10.15.202.120/das_input/Solution/ESSD_Performance"
dirsIometer = os.listdir(path);
folderHeaders = ['MEASURE_DT','VENDOR','PRODUCT_NAME', 'CONTROLLER', 'NAND_TECH','CELL_TYPE', 'FORM_FACTOR','CAPACITY','FIRMWARE','SLC_BUFFER','SERIAL_NUMBER','TEST_COUNT']
# folderHeaders = ['MEASURE_DT']
gFolderInfos = []
filesIometer = []
for subdir in dirsIometer:
    if subdir == 'parsed':
        continue
    pathIometer = path + '/' + subdir
    dirIometer = os.listdir(pathIometer)
    
    #folderInfos
    gFolderInfos.append({"folderName":subdir,"data":dict(zip(folderHeaders, subdir.split("_")))});
    
    for file in dirIometer:
        fileObj = {"filePath":pathIometer + '/' + file,"folderName":subdir,"fileName":file}
        filesIometer.append(fileObj)

# 01.Max Throughput.csv
for file in filesIometer:
    f = open(file['filePath'], 'r')
     
#      d_reader = csv.DictReader(f)
#      headers = d_reader.fieldnames
#      for line in d_reader:
#          matrix.append(line)
     
    #folderInfo
    folderInfos = file['folderName'].split("_")
    dicFolder = dict(zip(folderHeaders, folderInfos))
    csvReader = csv.reader(f)
    header = []
    i = 0
    for row in csvReader:
        if i == 0 :
            header = row
            
        if i > 0 :       
            dictionary = dict(zip(header, row))
            dictionary["meta"] = {}
            dictionary["meta"]["fileName"] = file['fileName'] #"Max Throughput.csv"
            dictionary["meta"]["folderName"] = file['folderName']
            dictionary["meta"]["tool"] = 'result_HBA'
             
            #header infos
            for head in folderHeaders:
                dictionary["meta"][head] = dicFolder[head]
             
            dictionary["keys"] = {}
            #dictionary["keys"]["folderName"] = file["folderName"] + "_" + dictionary["HostSystem"] + "_" + dictionary["Product"] + "_" + dictionary["Density"] + "_" + dictionary["Hardware"] + "_" + dictionary["Firmware"]
            dictionary["keys"]["spec"] = dictionary["Pre-Cond"] + "_" + dictionary["Blocksize(KB)"] + "_" + dictionary["Rnd/Seq"] + "_" + dictionary["RW Ratio"]  #+ "_" + dictionary["IOPS"]
            
            dictionary["keys"]["QueueDepth"] = dictionary["QueueDepth"]
            del dictionary["QueueDepth"]
             
            matrix.append(dictionary)
#             print dictionary
        i = i + 1
    f.close()
     
# print len(matrix)
try:
     
    vProduct = '' 
    con_str = 'swdashboard/swdashboard@166.125.19.99:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()

    for folders in gFolderInfos:   
        i = 0
         
        query = "delete from PERFORMANCE_FOLDER where " 
        query += " FOLDER_NAME ='" + folders['folderName'] +"'"
        cur.execute(query)
         
        query = "delete from PERFORMANCE where " 
        query += " FOLDER_NAME ='" + folders['folderName'] +"'"
#         query += " and tool = 'Iometer'"
        cur.execute(query)
         
        query = "insert into PERFORMANCE_FOLDER(FOLDER_NAME, MEASURE_DT, VENDOR, PRODUCT_NAME, CONTROLLER, NAND_TECH, CELL_TYPE, FORM_FACTOR, CAPACITY, FIRMWARE, SLC_BUFFER, SERIAL_NUMBER, TEST_COUNT, CATEGORY)"
        query += " values("
        query += "'" + folders['folderName'] + "'"
        for head in folderHeaders:
            query += ",'" + folders['data'][head] + "'" 
            i = i + 1
        
        query += ",'ESSD'"     
        query += ")"
        cur.execute(query)
    #con.commit()  
     
    i = 0
    prekey = ""
    for m in matrix:
        i = i+1
        print repr(i) + '(' + repr(len(matrix)) + ')'
        for mKey in m: 
            if type(m[mKey]) is dict:
                continue
#             
            query = " insert into PERFORMANCE(FOLDER_NAME, TOOL, DATA_SRC, SPEC, SPEC_LABEL, QUEUE_DEPTH, FIELD, MEASURE)"
            query += " values('" + m["meta"]['folderName'] +"','" + m["meta"]['tool'] +"','" + m["meta"]['fileName'] +"', '" + m["keys"]['spec'] +"','', '" + m["keys"]["QueueDepth"] +"', '"+ mKey +"',  '" + m[mKey] + "'  )"
            
            cur.execute(query)
        
    con.commit()
    #Iometer End
          
    # file move to parsed
    for folders in gFolderInfos:
        src = path + '/' + folders['folderName']
        dst = path + '/parsed/' + folders['folderName']
        if os.path.exists(dst):   
            shutil.rmtree(dst)
        shutil.copytree(src,dst)
        shutil.rmtree(src)
    
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