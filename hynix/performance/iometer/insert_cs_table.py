import sys, os, csv, urllib
import cx_Oracle
# Get index.csv
cs_url = "http://codesonar"
cs_additional_url = ".skhynix.com:7340"
# analysis_info structure
# prj_name / analysis_date / loc / csv_num / csv_url / codesonar_svr /P0_cnt / P1_cnt / P2_cnt / is_valid / 
cs_table=[]

cs_index = 0
for cs_index in range(2) :
    analysis_data=[]
    if cs_index == 0:
        cs_url = cs_url + '1' + cs_additional_url
    elif cs_index == 1:
        cs_url = "http://codesonar"
        cs_url = cs_url + '2' + cs_additional_url

    index_url = cs_url+'/index.csv' 
    print index_url
    
    urllib.urlretrieve(index_url, 'index.csv')

    with open('index.csv', 'r') as csvfile:
        index_data = csv.reader(csvfile)
        index = 0
        for index_data_row in index_data:
            if index == 0 :
                index += 1
                continue
            if index_data_row[1] == 'Finished':
                del index_data_row[1]
                index_data_row[1] = index_data_row[1][6:]+'/' + index_data_row[1][:5]
                print index_data_row
                analysis_data.append(index_data_row)
    
# open "index.csv" file and parse the analysis information
    index = 0
    for analysis_data_row in analysis_data:
        if cs_index == 0:
            csv_url = cs_url + analysis_data_row[3]
        elif cs_index == 1:
            csv_url = cs_url + analysis_data_row[3]
            
        urllib.urlretrieve(csv_url, 'analysis_data.csv')

        P0 = 0
        P1 = 0
        P2 = 0

        with open('analysis_data.csv', 'r') as csvfile:
            analysis_csv = csv.reader(csvfile)
            for analysis_csv_row in analysis_csv :
                if analysis_csv_row[6] == 'P0: High':
                    P0 += 1
                elif analysis_csv_row[6] == 'P1: Medium':
                    P1 += 1
                elif analysis_csv_row[6] == 'P2: Low':
                    P2 += 1
            #del analysis_data_row[3]
            analysis_data_row[3] = analysis_data_row[3][10:analysis_data_row[3].find('.csv')]
            
            if cs_index < 10:
                analysis_data_row.append(str(cs_index+1))
            analysis_data_row.append(str(P0))
            analysis_data_row.append(str(P1))
            analysis_data_row.append(str(P2))
            cs_table.append(analysis_data_row)

# Insert into DB

try:
    con_str = 'swdashboard/swdashboard@166.125.19.98:1521/APS'
    con = cx_Oracle.connect(con_str)
    cur = con.cursor()

    for cs_table_row in cs_table:
        print cs_table_row

#cur.execute('select * from tab ')
    i=0
    for cs_table_row in cs_table:
        cs_table_row[2] = int(cs_table_row[2])
        #cs_query = "insert into swdashboard.codesonar_test (PJT_CODE, PJT_NAME, ANALYSYS_DATE, LOC, P0_CNT, P1_CNT, P2_CNT, IS_RECENT) values (\'" + cs_table_row[0] + "\',\'" + cs_table_row[0] + "\',\'" + cs_table_row[1] + "\'," + str(cs_table_row[2]) + "," + str(cs_table_row[3]) + "," + str(cs_table_row[4]) + "," + str(cs_table_row[5]) + ", 'T')"
        # prj_code / prj_name / analysis_date / loc / csv_num / codesonar_svr /P0_cnt / P1_cnt / P2_cnt / is_valid /
        #  0           0          1             2     3          4             5        6        7        8

        # Check using select query
        cs_select_query = "select csv_num, codesonar_svr from swdashboard.codesonar_test where csv_num="+cs_table_row[3] +" and codesonar_svr="+cs_table_row[4]
        #cs_select_query = "select csv_num, codesonar_svr from swdashboard.codesonar_test where csv_num=1 and codesonar_svr=1"
        print "select Query : " + cs_select_query
        cur.execute(cs_select_query)
        temp = []
        temp = cur.fetchall()
        #print temp
        #print "len(temp)" + str(len(temp))
        if len(temp) == 1:
            print "Exist"
            continue

        else :
            cs_insert_query = "insert into swdashboard.codesonar_test (PJT_CODE, PJT_NAME, ANALYSYS_DATE, LOC, CSV_NUM, CODESONAR_SVR, P0_CNT, P1_CNT, P2_CNT, IS_VALID) values (\'"+ cs_table_row[0] + "\',\'" + cs_table_row[0] + "\',\'" + cs_table_row[1] + "\'," + str(cs_table_row[2]) + "," + cs_table_row[3] + ",\'"+ cs_table_row[4] + "\'," + cs_table_row[5] + "," + cs_table_row[6] + "," + cs_table_row[7]+ ",'T')"
            #cs_query = "select * from tab"
            print cs_insert_query
            cur.execute(cs_insert_query)
                
        i+=1
        #print cs_insert_query
        con.commit()
        #print cs_table_row
except cx_Oracle.DatabaseError:
    print "Database Error"

finally :
    if cur:
        cur.close()
    if con:
        con.close()


# Step 4 : insert information about codesonar
# Database structure
#-----------------------------------------------------------------
# PJT_NAME \t ANAYSIS_DATE \t PO_CNT \t P1_CNT \t P2_CNT
#-----------------------------------------------------------------

#select id,prj_files_xml,project_id,warning_count from cs_analysis where most_recent='True'
#Copy (select id, created, name_xml from cs_analysis where most_recent='True') To 'C:\Users\Administrator\Desktop\Test\output.csv' with CSV;
#psql -p 42060 -w -d cshub
