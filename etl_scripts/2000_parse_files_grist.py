
#parse grist datasets



import pandas as pd
import os
import sys
#import time
#from sqlalchemy import create_engine

# add parse function
xPath_package = '/home/mike/PycharmProjects/grant_db/'
sys.path.append(xPath_package)

from etl import etl_parse_grist as ETL_GRIST


# PATH to datasets
xFld_Path_dset_original = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/'
xFld_Path_set = xFld_Path_dset_original + '7000_GRIST/'

#os.listdir(xFld_Path_set)

xFld_Path_parsed = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/2000_dset_parsed/'
xDB = xFld_Path_parsed + 'grant_dset.db'
xDBCon = 'sqlite:///' + xDB


xlst_data_grant = []
xlst_data_org   = []
xlst_data_person = []

xlst_Funders = os.listdir(xFld_Path_set)


# we ignore WHO because of some problems in saving file 
# DOES NOT HELP 
#xls_Funders = xlst_Funders.remove('100004423')

#xFunder =  xlst_Funders[1] #'501100002428' # '501100000274' #'501100000381'

for xFunder in xlst_Funders:
    

    xFld_Data = xFld_Path_set  + xFunder
    lst_files = os.listdir(xFld_Data)
    
    print '---- processing ', xFunder, '# files:', len(lst_files)


    for xFile in lst_files:
        xMyFileName = xFld_Data + '/' + xFile
        q1 = ETL_GRIST.grist_parse_file (xMyFileName)

        if q1['xData_Grant']:
            xlst_data_grant.append(q1['xData_Grant'])
        if q1['xData_Inst']:
            xlst_data_org.append(q1['xData_Inst'])
        if q1['xData_Pers']:
            xlst_data_person.append(q1['xData_Pers'])
            

xdf_grant = pd.DataFrame(xlst_data_grant)
xdf_org = pd.DataFrame(xlst_data_org )
xdf_pers = pd.DataFrame(xlst_data_person)

print 'grants:', len(xdf_grant) , 'org:', len(xdf_org), 'pers:', len(xdf_pers)



# !!!! DOES NOT WORK 
# we just save a couple of recorsd to see 

xdf_grant.head(2000).to_sql(name = 'grist_grants',
           con = xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved grants'

xdf_org.to_sql(name = 'grist_org',
           con = xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved organisations'

xdf_pers.to_sql(name = 'grist_person',
           con = xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved person'


