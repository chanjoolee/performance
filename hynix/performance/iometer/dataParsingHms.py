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
from fileinput import filename
from operator import itemgetter, attrgetter

#Iometer start
#subprocess.call(r'net use z: /del', shell=True)
#subprocess.call(r'net use z: \\10.15.202.120\das_input /user:hynixad\probe test')

def fnMakeMapGeneral(dictionary):
    v_filename = dictionary["meta"]["fileName"]
    if v_filename == 'defect_lists.csv' :
        fnMakeMap_defectList(dictionary)
    elif v_filename == 'drive_level.csv' :
        fnMakeMap_driveLevel(dictionary)
    elif v_filename == 'erase_count.csv' :
        fnMakeMap_eraseCount(dictionary)
    elif v_filename == 'performance_data.csv' :
        fnMakeMap_performanceData(dictionary)
    elif v_filename == 'smart_attributes.csv' :
        fnMakeMap_smartAttributes(dictionary)
    elif v_filename == 'test_summary.csv' :
        fnMakeMap_testSummary(dictionary)
    
    
def fnMakeMap_defectList(dictionary):
    dictionary["keys"] = {}
    dictionary["keys"]["spec"] = dictionary["Subtest"] + "_" + dictionary["Serial number"] + "_" + dictionary["Capacity"] 
     
    matrix.append(dictionary)
    
def fnMakeMap_driveLevel(dictionary):
    dictionary["keys"] = {}
    dictionary["keys"]["spec"] = dictionary["Subtest"] + "_" + dictionary["Serial number"] + "_" + dictionary["Capacity"] 
    
def fnMakeMap_eraseCount(dictionary):
    dictionary["keys"] = {}
    dictionary["keys"]["spec"] = dictionary["Subtest"] + "_" + dictionary["Serial number"] + "_" + dictionary["Capacity"]
    
def fnMakeMap_performanceData(dictionary):
    dictionary["keys"] = {}
    dictionary["keys"]["spec"] = dictionary["Subtest"] + "_" + dictionary["Serial number"] + "_" + dictionary["Capacity"]
    
def fnMakeMap_smartAttributes(dictionary):
    dictionary["keys"] = {}
    dictionary["keys"]["spec"] = dictionary["Subtest"] + "_" + dictionary["Serial number"] + "_" + dictionary["Capacity"] + "_" + dictionary["Att"]
    
def fnMakeMap_testSummary(dictionary):
    dictionary["keys"] = {}
    dictionary["keys"]["spec"] = dictionary["Subtest"] + "_" + dictionary["Serial number"] + "_" + dictionary["Capacity"]
     
        
matrix = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''
path = "D:/hynix/requirement/HMS reliability Performance/parsing"
dirsIometer = os.listdir(path);
#folderHeaders = ['MEASURE_DT','VENDOR','PRODUCT_NAME', 'CONTROLLER', 'NAND_TECH','CELL_TYPE', 'FORM_FACTOR','CAPACITY','FIRMWARE','SLC_BUFFER','SERIAL_NUMBER','TEST_COUNT']
folderHeaders = ['CATEGORY','MEASURE_DT']
gFolderInfos = []
filesHms = []
for subdir in dirsIometer:
    if subdir == 'archive':
        continue
    pathHms = path + '/' + subdir
    dirHms = os.listdir(pathHms)
    
    #folderInfos
    gFolderInfos.append({"folderName":subdir,"data":dict(zip(folderHeaders, subdir.split("_")))});
    
    for file in dirHms:
        fileObj = {"filePath":pathHms + '/' + file,"folderName":subdir,"fileName":file}
        filesHms.append(fileObj)

# 01.Max Throughput.csv
for file in filesHms:
    f = open(file['filePath'], 'r')
     
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
            dictionary["meta"]["tool"] = 'HMS'
            for head in folderHeaders:
                dictionary["meta"][head] = dicFolder[head]
            fnMakeMapGeneral(dictionary)
            matrix.append(dictionary)
            
        i = i + 1
    f.close()
     
# print len(matrix)
try:
     
    vProduct = '' 
    con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
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
        query += "'" + folders['folderName'] + "'" # FOLDER_NAME
        query += ",'" + folders['data']['MEASURE_DT'] + "'" #MEASURE_DT
        query += ",'HMS'" # VENDOR
        query += ",'HMS'" # PRODUCT_NAME
        query += ",'HMS'"#CONTROLLER
        query += ",'HMS'"#NAND_TECH
        query += ",'HMS'"#CELL_TYPE
        query += ",'HMS'"#FORM_FACTOR
        query += ",'HMS'"#CAPACITY
        query += ",'HMS'"#FIRMWARE
        query += ",'HMS'"#SLC_BUFFER
        query += ",'HMS'"#SERIAL_NUMBER
        query += ",'HMS'"#TEST_COUNT
        query += ",'HMS'"     #CATEGORY
        query += ")"
        cur.execute(query)
    #con.commit()  
     
    i = 0
    prekey = ""
    #sorted(matrix, key=attrgetter('Serial number','Capacity','filename',))
    for m in matrix:
        i = i+1
        print repr(i) + '(' + repr(len(matrix)) + ')'
        for mKey in m: 
            if type(m[mKey]) is dict:
                continue
#             
            query = ""
            query += " declare"
            query += " vCnt number;"
            query += " begin" 
            query += "     select count(*) into vCnt from PERFORMANCE"
            query += "     where folder_name ='"+m["meta"]['folderName']+"'"
            query += "     and tool ='"+m["meta"]['tool']+"'"
            query += "     and DATA_SRC ='"+m["meta"]['fileName']+"'"
            query += "     and spec ='"+m["keys"]['spec']+"'"
            query += "     and QUEUE_DEPTH ='nodata'"
            query += "     and FIELD ='"+mKey+"';"
            query += "     if vCnt > 0 then "
            query += "         update PERFORMANCE set"
            query += "         measure = '" + m[mKey] + "'"
            query += "         where folder_name ='" +m["meta"]['folderName'] + "'"
            query += "         and tool ='" +m["meta"]['tool']+"'"
            query += "         and DATA_SRC ='" +m["meta"]['fileName']+ "'"
            query += "         and spec ='" +m["keys"]['spec']+"'"
            query += "         and QUEUE_DEPTH ='nodata'"
            query += "         and FIELD ='" +mKey+ "';"
            query += "     else"
            query += "         insert into PERFORMANCE(FOLDER_NAME, TOOL, DATA_SRC, SPEC, SPEC_LABEL, QUEUE_DEPTH, FIELD, MEASURE)"
            query += "         values('" + m["meta"]['folderName'] +"','" + m["meta"]['tool'] +"','" + m["meta"]['fileName'] +"', '" + m["keys"]['spec'] +"','', 'nodata', '"+ mKey +"',  '" + m[mKey] + "'  );"
            query += "     end if;"
            query += " end;"
            
            cur.execute(query)
        
    con.commit()
    #Iometer End
          
#     # file move to parsed
#     for folders in gFolderInfos:
#         src = path + '/' + folders['folderName']
#         dst = path + '/archive/' + folders['folderName']
#         if os.path.exists(dst):   
#             shutil.rmtree(dst)
#         shutil.copytree(src,dst)
#         shutil.rmtree(src)
    
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