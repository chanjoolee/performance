import os
import subprocess
import urllib
import csv
import cx_Oracle
import datetime

csUrl = "http://codesonar"
csAdditionalUrl = ".skhynix.com:7340"

################################################
# 1. get_index_csv()
# Description : 
################################################
def get_index_csv() :
	print "get_index_csv()"

	for csIndex in range(2) :
		analysisData = []
		csUrl = "http://codesonar"

		if csIndex == 0:
			csUrl = csUrl + "1" + csAdditionalUrl
		elif csIndex == 1:
			csUrl = csUrl + "2" + csAdditionalUrl

		indexUrl = csUrl + '/index.csv'
		urllib.urlretrieve(indexUrl, 'codesonar' + str(csIndex + 1) + '_index.csv')
	print "Download all index.csv from codesonar servers."


################################################
# 2. parse_index_csv()
# Description : 
################################################
def parse_index_csv() :
	print "parse_index_csv()"
	
	for index in range(2) :
		csvList = []
		csIndexFile = 'codesonar' + str(index+1) + '_index.csv'
		print csIndexFile
		with open(csIndexFile, 'r') as csvFile:
			indexData = csv.reader(csvFile)

			isFirstLine = True
			for indexDataRow in indexData :
				if isFirstLine == True :
					isFirstLine = False
					continue
				else:
					if indexDataRow[1] == 'Finished' :
						print "#################"
						projectName = indexDataRow[0]
						
						measureDate = indexDataRow[2]
						measureDate = measureDate.split('/')
						measureDate = measureDate[-1] + measureDate[0] + measureDate[1]

						csvNum = indexDataRow[-1].split('/')[-1].split('.')[0]
						projectCode = find_project_code(projectName)

						#print "Project Code : " 
						#print projectCode 
						if projectCode != 'None' :
							projectName = map_fwq_project_code(projectName, projectCode)
							
							meta = { 'measureDate': measureDate, 'projectCodeList' : projectCode, 'projectName': projectName , 'csvNum': csvNum , 'svrNum': str(index+1)}
							#meta['measureDate'] = change_measure_date(measureDate)
							functionSizePoint = calculate_project_point(csvNum, str(index+1), meta)
							functionSizePoint = round(functionSizePoint, 2)
							'''
							print measureDate
							print projectCode
							print projectName
							print csvNum
							print str(index+1)
							print functionSizePoint
							'''
							#projectName = map_fwq_project_code('Cepheus_Host_hil_exerciser', 'PJT_00021348')
							
							insert_result(measureDate, projectCode, projectName, csvNum, str(index+1), functionSizePoint)
							## need to be changed
							continue
							#break
						#else :
							## need to be changed
							#break

################################################
# 2-1. find_project_code()
# Description : 
################################################
def find_project_code(projectName) :			
	print "find_project_code()"
	print projectName
	projectCode = []
	try:
		connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
		connect = cx_Oracle.connect(connectInfo)
		cursor = connect.cursor()
		#print projectName
		selectQuery = "select PJT_NAME from PJT_MAPPING"
		whereCondition = "where LEGACY_NAME = 'CodeSonar' and LEGACY_PJT_NAME = '" + projectName + "'"
		selectQuery = selectQuery + " " + whereCondition
		#print selectQuery
		cursor.execute(selectQuery)
		projectCodeResult = cursor.fetchall()

		if len(projectCodeResult) >= 1 :
			#print projectCodeResult
			for eachProjectCode in projectCodeResult :
				#print eachProjectCode
				if eachProjectCode[0].split('_')[0] == 'PJT' :
					projectCode.append(eachProjectCode[0])
				else :
					print projectName + " is not mapped with PMS system"
					return "None"
			return projectCode
		else :
			print projectName + " project is need to be mapped first!!"
	


	except cx_Oracle.DatabaseError:
		print "Database Error"
	finally :
		if cursor :
			cursor.close()
		if connect :
			connect.close()

	return "None"

################################################
# 2-2. calculate_project_point()
# Description : 
################################################
def calculate_project_point(csvNum, svrNum, meta) :
	print "calculate_project_point()"
	
	analysisUrl = csUrl + svrNum + csAdditionalUrl
	metricPath = "/metric_search.csv?filter=2&metrics=LCode%3A100%2CvG%3A100&scope=aid%3A"
	analysisUrl = analysisUrl + metricPath + csvNum
	#analysisUrl = "http://codesonar1.skhynix.com:7340/metric_search.csv?metrics=LCode%3A100%2CvG%3A100&query=&prj_filter=29&filter=2&scope=aid%3A1319"
	print analysisUrl

	resp = urllib.urlretrieve(analysisUrl, 'analysis_report.csv')
	print resp

	with open('analysis_report.csv', 'r') as csvFile:
		metricData = csv.reader(csvFile)
		procedureCnt = 0
		below100Cnt = 0
		header = []
		delete_csv_raw(meta)
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
				meta['seq'] = i;
				if meta['projectName'].strip() != '' :
					insert_csv_raw(meta,dictionary)
				
			#print metricDataRow
			if len(metricDataRow) <= 1:
				#print len(metricDataRow)
				break
			if metricDataRow[0] == 'LCode' and metricDataRow[1] == 'Lines with Code' and metricDataRow[2] == 'Procedure' :
				procedureCnt += 1
				if int(metricDataRow[5]) <= 100 :
					below100Cnt += 1
				
			if metricDataRow[0] == 'vG' and metricDataRow[1] == 'Cyclomatic Complexity' and metricDataRow[2] == 'Procedure' :
				break
			else :
				continue

		print below100Cnt
		print procedureCnt

		if procedureCnt == 0 :
			return 0
		else :
			below100Portion = float(below100Cnt) / float(procedureCnt)

		print "below100Portion :"
		print below100Portion
		if below100Portion > 0.99 :
			slope = 1
			point = 20
		elif below100Portion > 0.95 :
			slope = 0.8
			point = 20.0 - slope*(1-below100Portion)*100
		elif below100Portion < 0.80 :
			slope = 0
			point = 0
		else :
			slope = 1
			point = 20.0 - slope*(1-below100Portion)*100
		
		print slope
		print point

	return point

################################################
# 2-3. map_fwq_project_code(projectName)
# Description : 
################################################
def map_fwq_project_code(projectName, projectCode) :
	print "map_fwq_project_code()"
	fwqProjectName = ''

	try:
		connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
		connect = cx_Oracle.connect(connectInfo)
		cursor = connect.cursor()
		
		selectQuery = "select LEGACY_PJT_NAME from PJT_MAPPING "
		whereCondition = "where LEGACY_PJT_KEY = '" + projectName + "' AND LEGACY_NAME = 'FWQ'"
		selectQuery = selectQuery + whereCondition
		#print selectQuery
		cursor.execute(selectQuery)
		#print "fwqProjectName is "
		fwqProjectName = cursor.fetchall()
		if len(fwqProjectName) == 0 :
			return ''
		else :
			fwqProjectName = fwqProjectName[0][0]
			print fwqProjectName
		connect.commit()

	except cx_Oracle.DatabaseError:
		print "Database Error"
	finally :
		if cursor :
			cursor.close()
		if connect :
			connect.close()

	if fwqProjectName is None : 
		return ''
	else :
		return fwqProjectName
		

################################################
# 2-4. insert_result()
# Description : 
################################################
def insert_result(measureDate, projectCodeList, projectName, csvNum, svrNum, functionSizePoint) :
	print "insert_result()"

	for projectCode in projectCodeList :
		try:
			connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
			connect = cx_Oracle.connect(connectInfo)
			cursor = connect.cursor()

			measureDate = change_measure_date(measureDate)
	
			insertQuery = "insert into FW_QUALITY_FUNC_SIZE(MEASURE_DT, PJT_CODE, PROJECT, CSV_NUM, CODESONAR_SVR, POINT)"
			values = " values ('" + measureDate + "', '" + projectCode + "', '" + projectName + "', '" + csvNum + "', '" + svrNum + "', " + str(functionSizePoint) + ")"



			insertQuery = insertQuery + values
			print insertQuery
			cursor.execute(insertQuery)
			connect.commit()
			print projectName + " is inserted successfully."
	
		except cx_Oracle.DatabaseError:
			print "Database Error"
		finally :
			if cursor :
				cursor.close()
			if connect :
				connect.close()
def delete_csv_raw(meta) :
	print "insert_result()"

	i = -1
	for projectCode in meta['projectCodeList'] :
		i = i + 1
		try:
			connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
			connect = cx_Oracle.connect(connectInfo)
			cursor = connect.cursor()

			measureDate = change_measure_date(meta['measureDate'])
	
			query = '''
	            declare 
	                vCount number;
	            begin
	                delete from FW_QUALITY_FUNC_SIZE_RAW 
	                where MEASURE_DT = \'''' +  meta['measureDate'] + '''\'
	                and PJT_CODE = \'''' +  projectCode + '''\'
	                and PROJECT = \'''' +  meta['projectName'] + '''\' ;	               
	            end;        
	        '''


			print query
			cursor.execute(query)
			connect.commit()
			print meta['projectName'] + " report csv is deleted successfully."
	
		except cx_Oracle.DatabaseError:
			print "Database Error"
		finally :
			if cursor :
				cursor.close()
			if connect :
				connect.close()
								
def insert_csv_raw(meta, row) :
	print "insert_result()"

	i = -1
	for projectCode in meta['projectCodeList'] :
		i = i + 1
		try:
			connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
			connect = cx_Oracle.connect(connectInfo)
			cursor = connect.cursor()

			measureDate = change_measure_date(meta['measureDate'])
	
			query = '''
	            declare 
	                vCount number;
	            begin
	                                
                    insert into FW_QUALITY_FUNC_SIZE_RAW(MEASURE_DT, PJT_CODE, PROJECT, SEQ, NAME, DESCRIPTION, GRANULARITY, FILE_NAME, PROCEDURE_NAME, VALUE)
                    values(
                        \'''' + meta['measureDate'] + '''\',
                        \'''' + projectCode + '''\',
                        \'''' + meta['projectName'] + '''\',
                        \'''' + str(meta['seq']) + '''\',
                        \'''' + row['Name'] + '''\',
                        \'''' + row['Description'] + '''\',
                        \'''' + row["Granularity"] + '''\',
                        \'''' + row['file'] + '''\',
                        \'''' + row['procedure'] + '''\',
                        \'''' + row['value'] + '''\'
                    );
	            end;        
	        '''


			print query
			cursor.execute(query)
			connect.commit()
			print meta['projectName'] + " report csv is inserted successfully."
	
		except cx_Oracle.DatabaseError:
			print "Database Error"
		finally :
			if cursor :
				cursor.close()
			if connect :
				connect.close()
				
################################################
# 2-4-1. change_measure_date()
# Description : 
################################################
def change_measure_date(measureDate):
	print "2-4-1. change_measure_date()"
	try:
		connectInfo = 'swdashboard/swdashboard@166.125.19.99:1521/GRID4DB'
		connect = cx_Oracle.connect(connectInfo)
		cursor = connect.cursor()
	
		selectQuery = "select TRUNC(sysdate, 'iw') + 1 from dual"
		#values = " values ('" + measureDate + "', '" + projectCode + "', '" + projectName + "', '" + csvNum + "', '" + svrNum + "', " + str(functionSizePoint) + ")"

		print selectQuery
		cursor.execute(selectQuery)
		changedDate = cursor.fetchall()
		changedDate = changedDate[0][0].strftime('%Y%m%d')

	except cx_Oracle.DatabaseError:
		print "Database Error"
	finally :
		if cursor :
			cursor.close()
		if connect :
			connect.close()
	return changedDate






################################################
# 0. Main
# Description : 
################################################



if __name__ == "__main__":
	print "Start Script"
	
	## 1. get index csv
	get_index_csv()
	## 2. get index csv
	parse_index_csv()
	

	
