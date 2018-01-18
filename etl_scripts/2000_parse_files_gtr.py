


'''
parsing and saving datafrom GTR
'''


import time
import sys
import os
import pandas as pd



# add parse function
xPath_package = '/home/mike/PycharmProjects/grant_db/'
sys.path.append(xPath_package)

from etl import etl_parse_gtr as GTR_PAR



xDB = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/2000_dset_parsed/grant_dset.db'
xDBCon = 'sqlite:///' + xDB


xFld_GTR = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/8000_GatewayResearch/'

# -- PERSONS

print '---start-- persons', time.ctime()
xFld_GTR_Data = xFld_GTR + '300_persons/'
xlst_data = os.listdir(xFld_GTR_Data)

print 'persons files:', len(xlst_data)

xlst_dict_persons = []

for xFileData in xlst_data:
    xFileName = xFld_GTR_Data + xFileData

    xlst_dict_persons = xlst_dict_persons + GTR_PAR.gtr_parse_person(xFileName)

print 'persons records:', len(xlst_dict_persons)

x_df = pd.DataFrame(xlst_dict_persons)

x_df.to_sql(name = 'gtr_persons',
           con = xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved, persons', time.ctime()


# projects 
print '---start-- projects', time.ctime()

# projects and links 

xFld_Gtr_projects = xFld_GTR + '100_projects/'

xlst_Files = os.listdir(xFld_Gtr_projects)


# Results list 
xlst_Res_proj = []
xlst_Res_links = []

for xFileName in xlst_Files:
    xFile = xFld_Gtr_projects + xFileName
    
    xq1 = GTR_PAR.gtr_parse_projects(xFile)
    
    xlst_Res_proj = xlst_Res_proj + xq1['grant_data']
    xlst_Res_links = xlst_Res_links + xq1['grant_links']

xdf_proj = pd.DataFrame(xlst_Res_proj)
xdf_proj.head()

xdf_links = pd.DataFrame(xlst_Res_links )
xdf_links.head()    

# daving 

print '----saving'

xdf_proj.to_sql(name = 'gtr_projects',
                con = xDBCon, 
                if_exists='replace', 
                index=True, index_label='record_id', 
                chunksize=10000)
print  'saved ! number_projects', len(xdf_proj)


xdf_links.to_sql(name = 'gtr_links',
                con = xDBCon, 
                if_exists='replace', 
                index=True, index_label='record_id', 
                chunksize=10000)

print  'number_links',    len(xdf_links)    

print 'saved, projects', time.ctime()

# Organisations 

#GTR Organisations 

print '---start-- Orgs', time.ctime()

xlst_Res_orgs = []


xFld_Gtr_orgs = xFld_GTR  + '200_orgs/'

xlst_Files = os.listdir(xFld_Gtr_orgs )

for xFileName in xlst_Files:
    xFile = xFld_Gtr_orgs + xFileName
    xq1 = GTR_PAR.gtr_parse_org(xFile)
    
    xlst_Res_orgs = xlst_Res_orgs + xq1 
    
  
xdf_orgs = pd.DataFrame(xlst_Res_orgs)


xdf_orgs.to_sql(name = 'gtr_orgs',
                con = xDBCon, 
                if_exists='replace', 
                index=True, index_label='record_id', 
                chunksize=10000)

print  'saved ! orgs',    len(xdf_orgs)   ,  time.ctime()

print 'ALL DONE'








