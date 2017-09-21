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


def recursive_overwrite(src, dest, ignore=None):
    if os.path.isdir(src):
        if not os.path.isdir(dest):
            os.makedirs(dest)
        files = os.listdir(src)
        if ignore is not None:
            ignored = ignore(src, files)
        else:
            ignored = set()
        for f in files:
            if f not in ignored:
                recursive_overwrite(os.path.join(src, f), 
                                    os.path.join(dest, f), 
                                    ignore)
    else:
        #if dest == '//10.15.202.120/das_sol/SSD_Regression/PJT_00036213/archive/T100HTA7.549035/PPlus.B1.S48.TLC.AB3.1TB_1.Recursive_1.HP.TCG.T100HTA7.549035.12_4583\\REGRESS-TP129-0\\00262_[test_Rebuild]_rebuild_3_limited_lba_range_larger_than_super_block_gc_long\\post_HostInfo.csv':
        #   test = 'test'
        try:
            shutil.copyfile(src, dest)
        except:
            pass
            
        
#         shutil.copy(src, dest)
        
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
            #gFolderInfosAll.append({"pathFolder": pathFolderName , "folderName":folderName, "pathPjtfolder":pathPjtfolder, "pjtfolder": pjtfolder ,"pathFirmware":pathFirmware,"firmware":firmware});
            # if is zip then unzip
            if os.path.isfile(pathFolderName) == True and folder_extension[1] == '.zip':
                #print pathFolderName
                ##gFolderInfosAll.append({"pathFolder": pathFirmware + '/' + os.path.splitext(folderName)[0] , "folderName":os.path.splitext(folderName)[0], "pathPjt":pathPjtfolder, "pjtfolder": pjtfolder ,"pathFirmware":pathFirmware,"firmware":firmware});
                #unzip(pathFolderName, pathFirmware + '/' + os.path.splitext(folderName)[0])
                a = "a"
            else :
                gFolderInfosAll.append({"pathFolder": pathFolderName , "folderName":folderName, "pathPjtfolder":pathPjtfolder, "pjtfolder": pjtfolder ,"pathFirmware":pathFirmware,"firmware":firmware});
                #saveFoldedr(pathFolderName)
            
            

# 01.process files: array list

try:
      
    #### REGRESSION_FOLDER
    
    #### REGRESSION_META
    
    #### REGRESSION   
   
    #Iometer End
           
    # file move to parsed
    print '================  target foler list =============='  
    for folders in gFolderInfosAll:
        pathArchive = folders['pathPjtfolder'] + '/archive'
        if not os.path.exists(pathArchive):
            os.makedirs(pathArchive)
        
        pathArchiveFirmware = pathArchive + '/' + folders['firmware']
        if not os.path.exists(pathArchiveFirmware):
            os.makedirs(pathArchiveFirmware)
            
            
        pathArchiveFolder = pathArchiveFirmware + '/' + folders['folderName']
        if not os.path.exists(pathArchiveFirmware):
            os.makedirs(pathArchiveFirmware)
        
        v_src = folders['pathFolder']
        v_dst = pathArchiveFolder
        
        print 'src:' + v_src
        print 'dst:' + v_dst
        
    
    print '=================  start move folder ================='  
    for folders in gFolderInfosAll:
        pathArchive = folders['pathPjtfolder'] + '\\archive'
        if not os.path.exists(pathArchive):
            os.makedirs(pathArchive)
        
        pathArchiveFirmware = pathArchive + '\\' + folders['firmware']
        if not os.path.exists(pathArchiveFirmware):
            os.makedirs(pathArchiveFirmware)
            
            
        pathArchiveFolder = pathArchiveFirmware + '\\' + folders['folderName']
        if not os.path.exists(pathArchiveFirmware):
            os.makedirs(pathArchiveFirmware)
        
        v_src = folders['pathFolder']
        v_dst = pathArchiveFolder
        
        print 'src:' + v_src
        print 'dst:' + v_dst
        
        recursive_overwrite(v_src,v_dst)
        
#         if os.path.exists(dst):   
#             shutil.rmtree(dst)
#         
#         shutil.copytree(src,dst)
        shutil.rmtree(v_src)
     
except cx_Oracle.DatabaseError ,ex:
    error, = ex.args    
    print query
    print 'Error Inserting Field Base'
    print 'Error.code    =', error.code
    print 'Error.message =', error.message
    print 'Error.offset  =', error.offset
   
     
finally :
    print 'completed!'  