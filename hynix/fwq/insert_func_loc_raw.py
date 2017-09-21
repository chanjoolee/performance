import os
import subprocess
import urllib
import csv
import cx_Oracle
import datetime

csUrl = "http://codesonar"
csAdditionalUrl = ".skhynix.com:7340"
query = ''

csvlist = []
def get_csv_list() :
	list = ''
	header = []
	try:
		connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
		connect = cx_Oracle.connect(connectInfo)
		cursor = connect.cursor()
		
		#selectQuery = "select MEASURE_DT, PJT_CODE, PROJECT, CSV_NUM, CODESONAR_SVR, LOC, POINT from FW_QUALITY_FUNC_SIZE where  CODESONAR_SVR > 0 order by MEASURE_DT, PJT_CODE, PROJECT"
		selectQuery = "select MEASURE_DT, PJT_CODE, PROJECT, CSV_NUM, CODESONAR_SVR, LOC, POINT from FW_QUALITY_FUNC_SIZE where MEASURE_DT = '20170905' and PROJECT = 'Columbus_FCPU' order by MEASURE_DT, PJT_CODE, PROJECT"
		#print selectQuery
		
		#cursor.execute(selectQuery)
		#changedDate = cursor.fetchall()
		
		
		list_meta = cursor.execute(selectQuery)
		list = cursor.fetchall()
		for h in list_meta.description:
			header.append(h[0])
		for m in list:			
			m1 = dict(zip(header, m))
			process_csv_list(cursor,m1)
		
		connect.commit()

	except cx_Oracle.DatabaseError ,ex:
		print "Database Error"
		print query
	finally :
		if cursor :
			cursor.close()
		if connect :
			connect.close()

def process_csv_list(cursor,meta) :
	try :
		analysisUrl = csUrl + str(meta['CODESONAR_SVR']) + csAdditionalUrl
		metricPath = "/metric_search.csv?filter=2&metrics=LCode%3A100%2CvG%3A100&scope=aid%3A"
		analysisUrl = analysisUrl + metricPath + str(meta['CSV_NUM'])
		
		print analysisUrl
	
		resp = urllib.urlretrieve(analysisUrl, 'analysis_report.csv')
		print resp
	
		
		if resp[1].type == 'text/csv':
			with open('analysis_report.csv', 'r') as csvFile:
				metricData = csv.reader(csvFile)
				
				header = []
				delete_csv_raw(cursor,meta)
				i = -1;
				for metricDataRow in metricData :
					i = i + 1;
					if i == 0 :
						header = metricDataRow
						j = 0
						for h in header :
							header[j] = h.strip()
							j = j + 1
					else:
						dictionary = dict(zip(header, metricDataRow))
						meta['seq'] = i
						insert_csv_raw(cursor,meta,dictionary)
	
	except cx_Oracle.DatabaseError:
		print "Database Error"
	finally :
		print "Continue"
			

def delete_csv_raw(cursor,meta) :
	
	query = '''
        declare 
            vCount number;
        begin
            delete from FW_QUALITY_FUNC_SIZE_RAW 
            where MEASURE_DT = \'''' +  meta['MEASURE_DT'] + '''\'
            and PJT_CODE = \'''' +  meta['PJT_CODE'] + '''\'
            and PROJECT = \'''' +  meta['PROJECT'] + '''\' ;	               
        end;        
    '''

	cursor.execute(query)

def insert_csv_raw(cursor,meta,dictionary) :

	query = '''
        declare 
            vCount number;
        begin
                            
            insert into FW_QUALITY_FUNC_SIZE_RAW(MEASURE_DT, PJT_CODE, PROJECT, SEQ, NAME, DESCRIPTION, GRANULARITY, FILE_NAME, PROCEDURE_NAME, VALUE)
            values(
                \'''' + meta['MEASURE_DT'] + '''\',
                \'''' + meta['PJT_CODE'] + '''\',
                \'''' + meta['PROJECT'] + '''\',
                \'''' + str(meta['seq']) + '''\',
                \'''' + dictionary['Name'] + '''\',
                \'''' + dictionary['Description'] + '''\',
                \'''' + dictionary["Granularity"] + '''\',
                \'''' + dictionary['file'] + '''\',
                \'''' + dictionary['procedure'] + '''\',
                \'''' + dictionary['value'] + '''\'
            );
        end;        
    '''
	cursor.execute(query)

	
    
################################################
# 0. Main
# Description : 
################################################



if __name__ == "__main__":
	print "Start Script"
	
	get_csv_list()
	

	
