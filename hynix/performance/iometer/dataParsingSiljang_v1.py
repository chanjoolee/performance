# -*- coding: ms949 -*-
'''
Created on 2017. 5. 29.

@author: lee chanjoo
'''
import xml.etree.ElementTree as ET
from openpyxl import load_workbook
import csv, itertools, cx_Oracle,datetime, os,fnmatch,  re , math , shutil, glob, codecs
from _sqlite3 import Row
from itertools import product
from telnetlib import theNULL
from test.test_datetime import DSTEND
import subprocess

subprocess.call(r'net use z: /del', shell=True)
subprocess.call(r'net use z: \\10.15.202.120\das_sol /user:hynixad\dfa dfa-dfa')

#Iometer start
files = []
summaries = []
vToday = datetime.datetime.now().strftime("%Y%m%d")
query = ''
# path = "\\177.179.133.22\Verification"
path = "\\10.15.202.120\das_sol\MOBILE_CORONA"
# rx = re.compile("(^(?P<key>[\w\s/]+):(?P<value>[\w\s\.-:\(\)]+)$)", re.MULTILINE)
rxSummary = re.compile("^##\s(?P<key>[a-zA-Z0-9 /\-]+)\s:\s(?P<value>.+)$", re.MULTILINE)
rxDetails = re.compile("^(?P<detail>[0-9]{4}.+)$", re.MULTILINE)
field_map = {"Test Board":"TEST_BOARD", "Sample":"SAMPLE", "Firmware":"FIRMWARE", "Script":"SCRIPT_NAME", "Sample Number":"SAMPLE_NUMBER", "Tester":"TESTER", "Test PC":"TEST_PC", "Port":"PORT", "Test Program version":"TEST_PROGRAM_VERSION", "Density":"DENSITY", "Start Time":"START_TIME", "End Time":"END_TIME", "Elapsed Time":"ELAPSED_TIME", "Total Item Count":"TOTAL_ITEM_COUNT", "Passed Item Count":"PASS_ITEM_COUNT", "Failed Item Count":"FAILED_ITEM_COUNT", "Last Failed Type":"LAST_FAILED_TYPE", "Script Folder":"SCRIPT_FOLDER"}
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
    matches = rxSummary.findall(filetext)
    mkey = {}
    summary = {} # summary data
    details = []
    ## start summay process
    for match in rxSummary.finditer(filetext):
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
    
    
        #01. 변수정의
        
        #02. 
        is_field_exist = False
        for key in field_map.keys():
            if(key == vKey) :
                is_field_exist = True
                break            
        if(is_field_exist == False): #만약 matching 되는 필드가 없으면 넘어간다.
            continue
            
        if(vKey == 'Tester'):
            continue
        
        #03 
        db_col = field_map[summary['key']];
        vValue = summary['value'] = unicode(match.group("value").strip(),"cp949").encode("utf-8")
        summary[db_col] = vValue
        
        
    summary['filePath'] = file['filePath']
    summary['fileName'] = file['filename']
    summary['yyyymm'] = filePathes[-3]
    summary['directory'] = filePathes[-2]
    ## end summary
    
    ## start details
    summary['details'] = details
    seq = 1
    for match in rxDetails.finditer(filetext):
        detail = {}
        detail['mkey'] = mkey
        rowText = match.group("detail").strip()
        rowTexts = rowText.split("|")
        detail['script'] = rowTexts[4].strip()
        detail['seq'] = seq
        detail['test_time'] = rowTexts[0].strip()
        detail['status'] = rowTexts[1].strip()
        detail['status_detail'] = rowTexts[2].strip()
        detail['duration'] = rowTexts[3].strip().split(' ')[0]
        
        
        details.append(detail)
        seq = seq + 1
        
        
        
    summaries.append(summary);
    
    


try:
    vProduct = '' 
    con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()
    i = 0
    prekey = ""
    for m in summaries:
        i = i+1
        query = '''
            declare 
                vCount number;
            begin
                select count(*) into vCount from CORONA_TEST_SUMMARY 
                where YYYYMM = \'''' +  m['yyyymm'] + '''\'
                and TEST_BOARD = \'''' +  m['TEST_BOARD'] + '''\'
                and SAMPLE = \'''' +  m['SAMPLE'] + '''\' 
                and FIRMWARE = \'''' +  m['FIRMWARE'] + '''\'
                and SCRIPT_NAME = \'''' +  m['SCRIPT_NAME'] + '''\'
                and SAMPLE_NUMBER = \'''' +  m['SAMPLE_NUMBER'] + '''\';
                
                
                if vCount > 0 then
                    update CORONA_TEST_SUMMARY set
                        TESTER = \'''' + m['TESTER'] + '''\'
                        , TEST_PC = \'''' + m['TEST_PC'] + '''\'
                        , PORT = \'''' + m['PORT'] + '''\'
                        , TEST_PROGRAM_VERSION = \'''' + m['TEST_PROGRAM_VERSION'] + '''\'
                        , DENSITY = \'''' + m['DENSITY'] + '''\'
                        , START_TIME = \'''' + m['START_TIME'] + '''\'
                        , END_TIME = \'''' + m['END_TIME'] + '''\'
                        , ELAPSED_TIME = \'''' + m['ELAPSED_TIME'] + '''\'
                        , TOTAL_ITEM_COUNT = \'''' + m['TOTAL_ITEM_COUNT'] + '''\'
                        , PASS_ITEM_COUNT = \'''' + m['PASS_ITEM_COUNT'] + '''\'
                        , FAILED_ITEM_COUNT = \'''' + m['FAILED_ITEM_COUNT'] + '''\'
                        , LAST_FAILED_TYPE = \'''' + m['LAST_FAILED_TYPE'] + '''\'
                        , FILE_NAME = \'''' + m['fileName'] + '''\'
                        , FILA_PATH = \'''' + m['filePath'] + '''\'
                        , SCRIPT_FOLDER = \'''' + m['SCRIPT_FOLDER'] + '''\'
                        , mod_dt = sysdate
                    where YYYYMM = \'''' +  m['yyyymm'] + '''\'
                    and TEST_BOARD = \'''' +  m['TEST_BOARD'] + '''\'
                    and SAMPLE = \'''' +  m['SAMPLE'] + '''\' 
                    and FIRMWARE = \'''' +  m['FIRMWARE'] + '''\'
                    and SCRIPT_NAME = \'''' +  m['SCRIPT_NAME'] + '''\'
                    and SAMPLE_NUMBER = \'''' +  m['SAMPLE_NUMBER'] + '''\';
                else
                    insert into CORONA_TEST_SUMMARY(YYMM, TEST_BOARD, SAMPLE, FIRMWARE, SCRIPT_NAME, SAMPLE_NUMBER, TESTER, TEST_PC, PORT, TEST_PROGRAM_VERSION, DENSITY, START_TIME, END_TIME, ELAPSED_TIME, TOTAL_ITEM_COUNT, PASS_ITEM_COUNT, FAILED_ITEM_COUNT, LAST_FAILED_TYPE, FILE_NAME, FILA_PATH, SCRIPT_FOLDER, PJT_CODE, INS_DT, MOD_DT)
                    values(
                        \'''' + m['yyyymm'] + '''\',
                        \'''' + m['TEST_BOARD'] + '''\',
                        \'''' + m['SAMPLE'] + '''\',
                        \'''' + m['FIRMWARE'] + '''\',
                        \'''' + m['SCRIPT_NAME'] + '''\',
                        \'''' + m['SAMPLE_NUMBER'] + '''\',
                        \'''' + m['TESTER'] + '''\',
                        \'''' + m["TEST_PC"] + '''\',
                        \'''' + m['PORT'] + '''\',
                        \'''' + m['TEST_PROGRAM_VERSION'] + '''\',
                        \'''' + m['DENSITY'] + '''\',
                        \'''' + m['START_TIME'] + '''\',
                        \'''' + m['END_TIME'] + '''\',
                        \'''' + m['ELAPSED_TIME'] + '''\',
                        \'''' + m['TOTAL_ITEM_COUNT'] + '''\',
                        \'''' + m['PASS_ITEM_COUNT'] + '''\',
                        \'''' + m['FAILED_ITEM_COUNT'] + '''\',
                        \'''' + m['LAST_FAILED_TYPE'] + '''\',
                        \'''' + m['fileName'] + '''\',
                        \'''' + m['filePath'] + '''\',
                        \'''' + m['SCRIPT_FOLDER'] + '''\',
                        null, -- pjtcode
                        sysdate,
                        null                        
                    );
                end if;
            end;        
        '''
        cur.execute(query)          
        for m1 in m['details'] :
            query =  '''
                declare 
                    vCount number;
                begin
                    select count(*) into vCount from CORONA_TEST_DETAIL 
                    where YYYYMM = \'''' +  m1['yyyymm'] + '''\'
                    and TEST_BOARD = \'''' +  m1['TEST_BOARD'] + '''\'
                    and SAMPLE = \'''' +  m1['SAMPLE'] + '''\' 
                    and FIRMWARE = \'''' +  m1['FIRMWARE'] + '''\'
                    and SCRIPT_NAME = \'''' +  m1['SCRIPT_NAME'] + '''\'
                    and SAMPLE_NUMBER = \'''' +  m1['SAMPLE_NUMBER'] + '''\'
                    and SCRIPT = \'''' +  m1['script'] + '''\';
                    
                    
                    if vCount > 0 then
                        update CORONA_TEST_DETAIL set
                            SEQ = \'''' + m1['seq'] + '''\'
                            , TEST_TIME = \'''' + m1['test_time'] + '''\'
                            , STATUS = \'''' + m1['status'] + '''\'
                            , STATUS_DETAIL = \'''' + m1['status_detail'] + '''\'
                            , DURATION = \'''' + m1['DENSITY'] + '''\'
                            , START_TIME = \'''' + m1['duration'] + '''\'
                            , mod_dt = sysdate
                        where YYYYMM = \'''' +  m1['yyyymm'] + '''\'
                        and TEST_BOARD = \'''' +  m1['TEST_BOARD'] + '''\'
                        and SAMPLE = \'''' +  m1['SAMPLE'] + '''\' 
                        and FIRMWARE = \'''' +  m1['FIRMWARE'] + '''\'
                        and SCRIPT_NAME = \'''' +  m1['SCRIPT_NAME'] + '''\'
                        and SAMPLE_NUMBER = \'''' +  m1['SAMPLE_NUMBER'] + '''\'
                        and SCRIPT = \'''' +  m1['script'] + '''\';
                    else
                        insert into SILJANG_TEST(YYYYMM, TEST_BOARD, SAMPLE, FIRMWARE, SCRIPT_NAME, SAMPLE_NUMBER, SCRIPT, SEQ, TEST_TIME, STATUS, STATUS_DETAIL, DURATION, INS_DT, MOD_DT)
                        values(
                            \'''' + m1['yyyymm'] + '''\',
                            \'''' + m1['TEST_BOARD'] + '''\',
                            \'''' + m1['SAMPLE'] + '''\',
                            \'''' + m1['FIRMWARE'] + '''\',
                            \'''' + m1['SCRIPT_NAME'] + '''\',
                            \'''' + m1['SAMPLE_NUMBER'] + '''\',
                            \'''' + m1['script'] + '''\',
                            \'''' + m1['seq'] + '''\',
                            
                            \'''' + m1['test_time'] + '''\',
                            \'''' + m1["status"] + '''\',
                            \'''' + m1['status_detail'] + '''\',
                            
                            sysdate,
                            null                        
                        );
                    end if;
                end;        
            '''
               
        #print query
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
