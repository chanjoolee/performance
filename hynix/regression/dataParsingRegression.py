'''
Created on 2017. 4. 13.

@author: lee chanjoo
'''

import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import csv, itertools, cx_Oracle,datetime, os, re , math , shutil , distutils
from _sqlite3 import Row
from itertools import product
from telnetlib import theNULL
from test.test_datetime import DSTEND
import subprocess
import zipfile
from encodings import undefined

def unzip(filepath,directory):
    zip_ref = zipfile.ZipFile(filepath,'r')
    zip_ref.extractall(directory)
    zip_ref.close()
    
    extention = os.path.splitext(filepath)
    saveFoldedr(extention[0])

def saveFoldedr(pathFolderName):
    slotfolers = os.listdir(pathFolderName)
    
#     v_folderName = ''
#     v_extention = os.path.splitext(folderName)
#     if v_extention[1] == '.zip':
#         v_folderName = v_extention[0]
#     else:
#         v_folderName = folderName        
    for slotfoler in slotfolers:
        pathslot = pathFolderName + '/' + slotfoler
        extension = os.path.splitext(pathslot)
        
        #if os.path.isfile(pathslot) == True and extension[1] != '.csv':
        #    continue
        if os.path.isfile(pathslot) == True and extension[1] == '':
            continue
        elif os.path.isfile(pathslot) == True and extension[1] == '.csv':
            # folder meta infos
            metaFile = open(pathslot, 'r');
            csvReader = csv.reader(metaFile)
            metaHeader = []
            i = 0
            j = 0
            for row in csvReader:
               
                if len(metaHeader) == 0 and row[0] != '' and row[0] != 'No' and row[0] != 'NO' :
                    if bool(re.match('^[\d].+\s(AM|PM)', row[0])): # not defined character
                        continue
                    colval = row[0].split(':',1)
                    col = colval[0].strip()
                    #col = col.replace(':', '')
                    val = ''
                    if len(colval) > 1 :
                        val = colval[1].strip()
                        
                    dictionary = {}
                    dictionary[col] = val
                    dictionary["meta"] = {}
                    dictionary["meta"]["pjtCode"] = pjtfolder
                    dictionary["meta"]["firmware"] = firmware.split('.')[0]
                    dictionary["meta"]["revision"] = firmware.split('.')[1]
                    dictionary["meta"]["folderName"] = v_folderName
                    dictionary["meta"]["rowkey"] = 'N'
                    dictionary["meta"]["rowvalue"] = 'N'
                    folderMetas.append(dictionary)
                else:
                    if j == 0 :
                        metaHeader = row
                    if j > 0 :
                        dictionary = dict(zip(metaHeader, row))
                        dictionary["meta"] = {}
                        dictionary["meta"]["pjtCode"] = pjtfolder
                        dictionary["meta"]["firmware"] = firmware.split('.')[0]
                        dictionary["meta"]["revision"] = firmware.split('.')[1]
                        dictionary["meta"]["folderName"] = v_folderName
                        
                        dictionary["meta"]["rowkey"] = 'no_category_testname'
                        dictionary["meta"]["rowvalue"] = dictionary['NO'].strip().zfill(5) + '_[' + dictionary['Category'] + ']_' +  dictionary['Test Name']                        
                        dictionary["no_category_testname"] = dictionary['NO'].strip().zfill(5) + '_[' + dictionary['Category'] + ']_' +  dictionary['Test Name']
                        folderMetas.append(dictionary)
                        
                    j = j + 1
                    
                
                
                i = i + 1
                
            metaFile.close()
            continue                
           
        testfolders = os.listdir(pathslot)
        
        for testfolder in testfolders:
            pathtest = pathslot + '/' + testfolder
            testFiles = os.listdir(pathtest)
            for testFile in testFiles:
       
                fileObj = {"filePath": pathtest + '/' + testFile,"pjtCode": pjtfolder ,"firmware":firmware.split('.')[0],"revision":firmware.split('.')[1], "folderName": v_folderName, "slotName":slotfoler, "testName":testfolder, "fileName":testFile}
                files.append(fileObj)
                
                
                
#Iometer start
subprocess.call(r'net use z: /del', shell=True)
subprocess.call(r'net use z: \\10.15.202.120\das_sol /user:hynixad\mobile ssd')

matrix = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''
path = "//10.15.202.120/das_sol/SSD_Regression"
dirs = os.listdir(path); 
folderHeaders = []
folderMetas = []
folderFirmwares = []
# folderHeaders = ['MEASURE_DT']
gFolderInfos = []
gFolderInfosAll = [] #zip file include
files = []

for pjtfolder in dirs:
    if pjtfolder == 'archive' or pjtfolder == 'PMS project  name':
        continue
    pathPjtfolder = path +  '/' + pjtfolder
    firmwares = os.listdir(pathPjtfolder)
    for firmware in firmwares:
        if firmware == 'archive' or firmware == 'UNDEFINED':
            continue
        # un need file skip
        v_extention_firm = os.path.splitext(firmware)
        if v_extention_firm[1] == '.zip':
            continue
        
        pathFirmware = pathPjtfolder + '/' + firmware
        folderFirmwares.append({"pathPjtfolder":pathPjtfolder,"pathFirmware":pathFirmware,"firmwareFolderName":firmware})
        folderNames = os.listdir(pathFirmware)     
            
        for folderName in folderNames:
            #folderInfos
            #gFolderInfos.append({"folderName":subdir,"data":dict(zip(folderHeaders, subdir.split(".")))});
            v_folderName = ''
            v_extention = os.path.splitext(folderName)
            if v_extention[1] == '.zip':
                v_folderName = v_extention[0]
            else:
                v_folderName = folderName   
                    
            
            
            pathFolderName = pathFirmware + '/'+ folderName # #PPlus.B1.S48.TLC.AA5.128GB.256GB.512GB.Recursive_1.HP.C200HT77.519330.1
            # zipfile
            folder_extension = os.path.splitext(pathFolderName)
            gFolderInfos.append({"pjtCode": pjtfolder , "firmware": firmware.split('.')[0] ,"revision": firmware.split('.')[1] ,"folderName": v_folderName});
            
            # if is zip then unzip
            if os.path.isfile(pathFolderName) == True and folder_extension[1] == '.zip':
                print pathFolderName + '/' + pathFirmware
                gFolderInfosAll.append({"pathFolder": pathFirmware + '/' + os.path.splitext(folderName)[0] , "folderName":os.path.splitext(folderName)[0], "pathPjt":pathPjtfolder, "pjtfolder": pjtfolder ,"pathFirmware":pathFirmware,"firmware":firmware, "iszip":False});
                gFolderInfosAll.append({"pathFolder": pathFolderName , "folderName":folderName, "pathPjtfolder":pathPjtfolder, "pjtfolder": pjtfolder ,"pathFirmware":pathFirmware,"firmware":firmware, "iszip":True});
                unzip(pathFolderName, pathFirmware + '/' + os.path.splitext(folderName)[0])
                #a = "a"
            else :
                gFolderInfosAll.append({"pathFolder": pathFolderName , "folderName":folderName, "pathPjtfolder":pathPjtfolder, "pjtfolder": pjtfolder ,"pathFirmware":pathFirmware,"firmware":firmware, "iszip":False});
                saveFoldedr(pathFolderName)
            
            

# 01.Max Throughput.csv
for file in files:
    f = open(file['filePath'], 'r')
      
#      d_reader = csv.DictReader(f)
#      headers = d_reader.fieldnames
#      for line in d_reader:
#          matrix.append(line)
      
    #folderInfo
#     folderInfos = file['folderName'].split("_")
#     dicFolder = dict(zip(folderHeaders, folderInfos))
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
            dictionary["meta"]["pjtCode"] = file['pjtCode']
            dictionary["meta"]["firmware"] = file['firmware']
            dictionary["meta"]["revision"] = file['revision']
            dictionary["meta"]["folderName"] = file['folderName']
            dictionary["meta"]["slotName"] = file['slotName']
            dictionary["meta"]["testName"] = file['testName']
            dictionary["meta"]["folderName"] = file['folderName']
            dictionary["meta"]["tool"] = 'regression'
            dictionary["meta"]["rowkey"] = header[0]
            dictionary["meta"]["rowvalue"] = row[0]
              
            #header infos
#             for head in folderHeaders:
#                 dictionary["meta"][head] = dicFolder[head]
              
#             dictionary["keys"] = {}
#             #dictionary["keys"]["folderName"] = file["folderName"] + "_" + dictionary["HostSystem"] + "_" + dictionary["Product"] + "_" + dictionary["Density"] + "_" + dictionary["Hardware"] + "_" + dictionary["Firmware"]
#             dictionary["keys"]["spec"] = dictionary["Pre-Cond"] + "_" + dictionary["Blocksize(KB)"] + "_" + dictionary["Rnd/Seq"] + "_" + dictionary["RW Ratio"]  #+ "_" + dictionary["IOPS"]
#             
#             dictionary["keys"]["QueueDepth"] = dictionary["QueueDepth"]
#             del dictionary["QueueDepth"]
              
            matrix.append(dictionary)
#             print dictionary
        i = i + 1
    f.close()
      
# print len(matrix)
try:
      
    vProduct = '' 
    con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()
    
    #### REGRESSION_FOLDER
    for folders in gFolderInfos:   
        i = 0
          
        query = "delete from REGRESSION_FOLDER where " 
        query += " PJT_CODE ='" + folders['pjtCode'] +"'"        
        query += " and FOLDER_NAME ='" + folders['folderName'] +"'"
        cur.execute(query)
        
        query = "delete from REGRESSION_META where " 
        query += " PJT_CODE ='" + folders['pjtCode'] +"'"
        query += " and FOLDER_NAME ='" + folders['folderName'] +"'"
        cur.execute(query)
          
        query = "delete from REGRESSION where " 
        query += " PJT_CODE ='" + folders['pjtCode'] +"'"
        query += " and FOLDER_NAME ='" + folders['folderName'] +"'"
#         query += " and tool = 'Iometer'"
        cur.execute(query)
          
        query = "insert into REGRESSION_FOLDER(PJT_CODE,FIRMWARE, REVISION,FOLDER_NAME, ins_dt)"
        query += " values("
        query += "'" + folders['pjtCode'] + "',"
        query += "'" + folders['firmware'] + "',"
        query += "'" + folders['revision'] + "',"
        query += "'" + folders['folderName'] + "',"
        query += " sysdate "
        query += ")"
        cur.execute(query)
    #con.commit()  
    
    
    #### REGRESSION_META
    i = 0
    prekey = ""
    for m in folderMetas:
        i = i+1
        print repr(i) + '(' + repr(len(matrix)) + ')'
         
        for mKey in m: 
            if type(m[mKey]) is dict or mKey == '':
                continue
            m[mKey] = m[mKey].replace("'","''")
            query = " insert into REGRESSION_META(PJT_CODE, FOLDER_NAME, ROWKEY, ROWVALUE, FIELD, MEASURE)"
            query += " values('" + m["meta"]['pjtCode'] + "','" + m["meta"]['folderName'] +"','"  + m["meta"]['rowkey'] + "', '" + m["meta"]['rowvalue'] + "','"  + mKey.strip() +"',  '" + m[mKey].strip() + "'  )"
            print query
            cur.execute(query)
    
    
    #### REGRESSION   
    i = 0
    prekey = ""
    for m in matrix:
        i = i+1
        print repr(i) + '(' + repr(len(matrix)) + ')'
         
        for mKey in m: 
            if type(m[mKey]) is dict:
                continue
            m[mKey] = m[mKey].replace("'","''")
            query = " insert into REGRESSION(PJT_CODE,FOLDER_NAME, SLOTNAME, TESTNAME, TOOL, DATA_SRC, ROWKEY, ROWVALUE, FIELD, MEASURE)"
            query += " values('" + m["meta"]['pjtCode'] + "','" + m["meta"]['folderName'] +"','" + m["meta"]['slotName'] +"','" + m["meta"]['testName'] +"','" + m["meta"]['tool'] + "','" + m["meta"]['fileName'] + "','"  + m["meta"]['rowkey'] + "', '" + m["meta"]['rowvalue'] + "','"  + mKey.strip() +"',  '" + m[mKey].strip() + "'  )"
            
            print query
            cur.execute(query)
         
    con.commit()
    #Iometer End
           
    # file move to parsed
    for folders in gFolderInfosAll:
        iszip = folders['iszip']
        src = folders['pathFolder']
        if iszip:
            pathArchive = folders['pathPjtfolder'] + '/archive'
            if not os.path.exists(pathArchive):
                os.makedirs(pathArchive)
            
            pathArchiveFirmware = pathArchive + '/' + folders['firmware']
            if not os.path.exists(pathArchiveFirmware):
                os.makedirs(pathArchiveFirmware)
                
                
            pathArchiveFolder = pathArchiveFirmware + '/' + folders['folderName']
            if not os.path.exists(pathArchiveFirmware):
                os.makedirs(pathArchiveFirmware)
            
            
            dst = pathArchiveFolder
            
            print 'src:' + src
            print 'dst:' + dst
            
            if os.path.exists(dst):   
                #shutil.rmtree(dst)
                os.remove(dst)
            
            #shutil.copytree(src,dst)
            shutil.copy(src,dst)
        
        shutil.rmtree(src)
     
except cx_Oracle.DatabaseError ,ex:
    
    for folders in gFolderInfosAll:
        src = folders['pathFolder']
        iszip = folders['iszip']
        if not iszip:           
            shutil.rmtree(src)
            
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
         
    print 'completed!'  