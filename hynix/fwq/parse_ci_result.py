################################################
# Import Part
################################################
import os
import csv
import sys
import subprocess
import cx_Oracle
import re
import glob
import shutil

################################################
# Global Variable
################################################


################################################           
# 1. measureCodingRuleScore()
# Description :
################################################
def measureCodingRuleScore(fileList):
    print "measureCodingRuleSocre()"
    removes = ['02_04','02_05','05_04','07_02','08_02','08_04','08_06','08_07',
               '08_09','08_11','08_13','10_01','10_04','11_04','11_05','12_01'
               ,'12_03','12_04','13_02','13_03','13_04','15_01','15_02','15_04','15_05','15_06'
               ,'17_01','17_08','18_04','18_05','19_02','20_01','20_05']
    #filterd directory
    if not os.path.exists('filtered'):
        os.makedirs('filtered')
        
    #delete filtered files    
    #vFilterfiles = glob.glob('filtered/*')
    #for f in vFilterfiles:
    #    os.remove(f)
       
    if not os.path.exists('parsed'):
        os.makedirs('parsed')
        
    for csvFile in fileList :
        print csvFile
        vDirectoryname = os.path.dirname(csvFile)     
        vFilename = os.path.basename(csvFile)
        if os.path.basename(vDirectoryname) == 'filtered':
            continue 
        if os.path.basename(vDirectoryname) == 'parsed':
            continue 
        
        csvRows = []
        with open(csvFile, 'rb') as csvResult:
            csvResult = csv.reader(csvResult)
            csvResult = filter(None, csvResult)
            
            criticalCnt = 0
            majorCnt = 0
            minorCnt = 0
            otherCnt = 0
            for csvRow in csvResult :
                matchRemove = False
                for remove in removes :
                    m = re.search(remove, csvRow[1])
                    if m != None :
                        matchRemove = True
                        break
                if matchRemove :
                    continue
                
                csvRows.append(csvRow)
                if csvRow[-1] == "Critical" :
                    criticalCnt+=1
                elif csvRow[-1] == "Major" :
                    majorCnt+=1
                elif csvRow[-1] == "Minor" :
                    minorCnt+=1
                else :
                    otherCnt+=1
        
        with open('filtered/' + vFilename,'wb') as csvfile1:
            spamwriter = csv.writer(csvfile1, delimiter=',',quotechar='"', quoting=csv.QUOTE_MINIMAL)
            spamwriter.writerows(csvRows)
        
        
        if os.path.isfile('parsed/' + vFilename):
            os.remove('parsed/' + vFilename);
        
        shutil.copyfile(csvFile, 'parsed/' + vFilename)
        os.remove(csvFile)
            
        print "Cricital CNT is " + str(criticalCnt)
         
        qualityProjectName = csvFile.split('\\')[-1].split('.')[0]
         
        # findLoc Val
        recentData = getRecentLOC(qualityProjectName)
                  
        # calc the Score
        if recentData == 0 :
            print "Please check the LOC value in CodeSonar."
        else :
            PPM = (float(criticalCnt) / float(recentData[-2])) * 1000000
          
            if PPM > 300.0 :
                weeklyPoint = 0
            else :
                weeklyPoint = 20 / 300 * (300 - PPM)
            insertRow = list(recentData[:3])
              
            insertRow.append(recentData[-2])
            insertRow.append(criticalCnt)
            insertRow.append(majorCnt)
            insertRow.append(minorCnt)
            insertRow.append(weeklyPoint)
              
            print insertRow        
          
  
            insertData(insertRow)
    
        
def moveFiles(path):     
    if os.path.exists(path + '/parsed'):   
        shutil.rmtree(path + '/parsed')
    shutil.copytree(path + '/Output',path + '/parsed')    
    vFilterfiles = glob.glob(path + '/Output/*')
    for f in vFilterfiles:
        os.remove(f)
    
################################################           
# 1.1. getRecentLOC()
# Description :
################################################    
def getRecentLOC(qualityProjectName):
    print "1.1. getRecentLOC()"
    
    try :
        connectInfo = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
        #connectInfo = 'swdashboarddev/swdashboarddev@166.125.112.110:1521/ALTDEV'
        connect = cx_Oracle.connect(connectInfo)
        cursor = connect.cursor()
        
#         selectQuery = '''with maxdt as (select project, MAX(MEASURE_DT) AS Latest from FW_QUALITY_STATIC_ANALISIS group by project order by 1)
# select a.* from maxdt  
# join FW_QUALITY_STATIC_ANALISIS a on a.project = maxdt.project and maxdt.Latest = MEASURE_DT
# where a.project = '''
#         selectQuery = selectQuery + "'" + qualityProjectName + "'"
        selectQuery = '''
            with maxdt as (select project, MAX(MEASURE_DT) AS Latest from FW_QUALITY_STATIC_ANALISIS group by project order by 1)
            select 
                a.MEASURE_DT, 
                a.PJT_CODE, 
                a.PROJECT, 
                a.CSV_NUM, 
                a.CODESONAR_SVR, 
                a.P0_CNT, 
                a.LOC, 
                a.POINT 
            from (
                select a.* ,
                    row_number() over(order by a.eds desc, a.loc desc)  row_num
                from (
                    select 
                        a.MEASURE_DT, 
                        a.PJT_CODE, 
                        a.PROJECT, 
                        a.CSV_NUM, 
                        a.CODESONAR_SVR, 
                        a.P0_CNT, 
                        a.LOC, 
                        a.POINT,
                        UTL_MATCH.edit_distance_similarity(a.project,\'''' + qualityProjectName + '''\') eds
                    from maxdt  
                    join FW_QUALITY_STATIC_ANALISIS a 
                    --join FW_QUALITY_POINT a
                        on a.project = maxdt.project
                    and maxdt.Latest = MEASURE_DT
                ) a
            ) a
            where row_num = 1'''
        print selectQuery
        
        cursor.execute(selectQuery)
        selectResult = cursor.fetchall()
        
        if not selectResult :
            recentLOC = 0
        else :
            recentLOC = selectResult[0]
        
    except cx_Oracle.DatabaseError:
        print "Database Error"
    finally :
        if cursor :
            cursor.close()
        if connect :
            connect.close()            
        
    return recentLOC
################################################           
# 1.2. insertData()
# Description :
################################################
def insertData(insertRow) :
    print "1.2. insertData()"
    
    try :
        connectInfo = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
        #connectInfo = 'swdashboarddev/swdashboarddev@166.125.112.110:1521/ALTDEV'
        connect = cx_Oracle.connect(connectInfo)
        cursor = connect.cursor()
        
        print insertRow
#         insertQuery = "insert into FW_QUALITY_CODING_RULE ( MEASURE_DT, PJT_CODE, PROJECT, LOC, A, B, C, POINT) VALUES ('"
#         insertQuery = insertQuery +  insertRow[0] + "', '" + insertRow[1] + "', '" + insertRow[2] + "', " + str(insertRow[3]) + ", " + str(insertRow[4]) + ", " + str(insertRow[5]) + ", " + str(insertRow[6]) + ", " + str(insertRow[7]) + ")"
#         print insertQuery
#         
#         cursor.execute(insertQuery)
        insertQuery = '''
            declare 
                vCount number;
            begin
                select count(*) into vCount from FW_QUALITY_CODING_RULE 
                where measure_dt = \'''' +  insertRow[0] + '''\'
                and PJT_CODE = \'''' +  insertRow[1] + '''\'
                and project = \'''' +  insertRow[2] + '''\' ;
                
                if vCount > 0 then
                    update FW_QUALITY_CODING_RULE set
                        loc = \'''' + str(insertRow[3]) + '''\'
                        , A = ''' +  str(insertRow[4]) + '''
                        , B = ''' +  str(insertRow[5]) + '''
                        , C = ''' +  str(insertRow[6]) + '''
                        , POINT = ''' +  str(insertRow[7]) + '''
                        , mod_dt = sysdate
                    where measure_dt = \'''' +  insertRow[0] + '''\'
                    and PJT_CODE = \'''' +  insertRow[1] + '''\'
                    and project = \'''' +  insertRow[2] + '''\' ;
                else
                    insert into FW_QUALITY_CODING_RULE (MEASURE_DT, PJT_CODE, PROJECT, LOC, A, B, C, POINT, REAL_MEASURE_DT, INS_DT)
                    values(
                        \'''' + insertRow[0] + '''\',
                        \'''' + insertRow[1] + '''\',
                        \'''' + insertRow[2] + '''\',
                        ''' + str(insertRow[3]) + ''',
                        ''' + str(insertRow[4]) + ''',
                        ''' + str(insertRow[5]) + ''',
                        ''' + str(insertRow[6]) + ''',
                        ''' + str(insertRow[7]) + ''',
                        \'''' + insertRow[0] + '''\',
                        sysdate                        
                    );
                end if;
            end;
        
        '''
        
        
        print insertQuery
        
        cursor.execute(insertQuery)
        connect.commit()
        
        
    except cx_Oracle.DatabaseError:
        print "Database Error"
    finally :
        if cursor :
            cursor.close()
        if connect :
            connect.close()            
        
################################################
# 0. Main
# Description : 
################################################
if __name__ == "__main__":
    print "Start Script"
    print "1. Find Code Inspector Results"
    path = "D:/hynix/requirement/FWQualityMetric/CodingRule/parse_ci_result"
    os.chdir(path)
    fileList = subprocess.check_output(["dir", "/s", "/b", "*.csv"], shell=True)
    fileList =  fileList.split("\r\n")[:-1]
        
    print "2. Parse the CSV file Results"
    
    measureCodingRuleScore(fileList)
    
    #moveFiles(path);
    
    
