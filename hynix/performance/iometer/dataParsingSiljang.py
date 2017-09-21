# -*- coding: ms949 -*-
'''
Created on 2017. 2. 16.

@author: lee chanjoo
'''
import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import csv, itertools, cx_Oracle,datetime, os,fnmatch,  re , math , shutil, glob, codecs
from _sqlite3 import Row
from itertools import product
from telnetlib import theNULL
from test.test_datetime import DSTEND

#Iometer start
files = []
matrix = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''
# path = "\\177.179.133.22\Verification"
path = "D:/hynix/requirement/Siljang_Test/eMMC Log"
# rx = re.compile("(^(?P<key>[\w\s/]+):(?P<value>[\w\s\.-:\(\)]+)$)", re.MULTILINE)
rx = re.compile("^(?P<key>[a-zA-Z0-9 /\-]+)\s:\s(?P<value>.+)$", re.MULTILINE)
for root, dirnames, filenames in os.walk(path):
    for filename in fnmatch.filter(filenames, '*_SR*.log'):
        fileinfo = {}
        fileinfo['filePath'] = os.path.join(root, filename)
        fileinfo['filename'] = filename        
        files.append(fileinfo)
        
     
for file in files:
    filePathes = file['filePath'].split('\\')
    f = open(file['filePath'], 'r')
    filetext = f.read()
    f.close()
    #matches = re.findall("(^(?P<key>[\w\s]+):(?P<value>.+)$)", filetext)
    matches = rx.findall(filetext)
    mkey = {}
    for match in rx.finditer(filetext):
        vKey = match.group("key").strip()
        vValue = match.group("value").strip()        
        if(vKey == 'Test Board'):
            mkey['testBoard'] = vValue
        
        if(vKey == 'Sample'):
            mkey['sample'] = vValue
            
        if(vKey == 'Firmware'):
            mkey['firmware'] = vValue
            
        if(vKey == 'Sample Number'):
            mkey['sampleNumber'] = vValue
        
    for match in rx.finditer(filetext):
        #01. 변수정의
        m = {}
        m['keys'] = mkey
        
        vKey = match.group("key").strip()
        vValue = match.group("value").strip()
        
        #02. 
        iskey = False
        for key in mkey.keys():
            if(key == vKey) :
                iskey = True
                break            
        if(iskey):
            continue
            
        if(vKey == 'Tester'):
            continue
        
        #03 
        m['key'] = match.group("key").strip()
        m['value'] = unicode(match.group("value").strip(),"cp949").encode("utf-8")
        m['filePath'] = file['filePath']
        m['fileName'] = file['filename']
        m['yyyymm'] = filePathes[-3]
        m['directory'] = filePathes[-2]
        matrix.append(m);


try:
    vProduct = '' 
    con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()
    i = 0
    prekey = ""
    for m in matrix:
        i = i+1
            
        query = '''
            declare 
                vCount number;
            begin
                select count(*) into vCount from SILJANG_TEST 
                where YYYYMM = \'''' +  m['yyyymm'] + '''\'
                and TEST_BOARD = \'''' +  m['keys']['sample'] + '''\'
                and SAMPLE = \'''' +  m['keys']['sample'] + '''\' 
                and FIRMWARE = \'''' +  m['keys']['firmware'] + '''\'
                and SAMPLE_NUMBER = \'''' +  m['keys']['sampleNumber'] + '''\'
                and FIELD = \'''' +  m['key'] + '''\';
                
                if vCount > 0 then
                    update SILJANG_TEST set
                        VALUE = \'''' + m['value'] + '''\'
                        , mod_dt = sysdate
                    where YYYYMM = \'''' +  m['yyyymm'] + '''\'
                    and TEST_BOARD = \'''' +  m['keys']['sample'] + '''\'
                    and SAMPLE = \'''' +  m['keys']['sample'] + '''\' 
                    and FIRMWARE = \'''' +  m['keys']['firmware'] + '''\'
                    and SAMPLE_NUMBER = \'''' +  m['keys']['sampleNumber'] + '''\'
                    and FIELD = \'''' +  m['key'] + '''\';
                else
                    insert into SILJANG_TEST(YYYYMM, TEST_BOARD, SAMPLE, FIRMWARE, SAMPLE_NUMBER, FIELD, VALUE, DIRECTORY, FILE_NAME, INS_DT, MOD_DT)
                    values(
                        \'''' + m['yyyymm'] + '''\',
                        \'''' + m['keys']['testBoard'] + '''\',
                        \'''' + m['keys']['sample'] + '''\',
                        \'''' + m['keys']['firmware'] + '''\',
                        \'''' + m['keys']['sampleNumber'] + '''\',
                        \'''' + m['key'] + '''\',
                        \'''' + m["value"] + '''\',
                        \'''' + m['directory'] + '''\',
                        \'''' + m['fileName'] + '''\',
                        sysdate,
                        null                        
                    );
                end if;
            end;        
        '''
            
        print query
        cur.execute(query)
    con.commit()
      
except cx_Oracle.DatabaseError ,ex:
    error, = ex.args        
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
