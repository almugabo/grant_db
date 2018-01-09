# coding: utf-8

# # Parse files from the European Union Funding
# ### for now only FP7 and H2020 , to add later all FP6 to FP1
#

import pandas as pd
# import os
import sys
import time

# from sqlalchemy import create_engine

# add parse function
xPath_package = '/home/mike/PycharmProjects/grant_db/'
sys.path.append(xPath_package)

from etl import etl_parse_eu as ETP_EU
from etl import etl_utils as UT

# PATH to datasets
xFld_Path_dset_original = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/'
xFld_Path_set = xFld_Path_dset_original + '6000_EU/'

# os.listdir(xFld_Path_set)

xFld_Path_parsed = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/2000_dset_parsed/'
xDB = xFld_Path_parsed + 'grant_dset.db'
xDBCon = 'sqlite:///' + xDB

# In[ ]:

x_res_xlst_projects = []
x_res_xlst_projects_prog = []
x_res_xlst_projects_org = []

xzip_File = xFld_Path_set + 'cordis-fp7projects-xml.zip'

print 'start--', time.ctime()
xq1 = UT.read_zipped_file(xzip_File)
for xnr, xq2 in enumerate(xq1):
    # for testing
    # if xnr == 1000:
    #    break

    # process
    qq1 = ETP_EU.eu_fp7_getprojectdata(xq2)

    x_res_xlst_projects.append(qq1['data_project'])

    for xprog in qq1['data_project_prog']:
        x_res_xlst_projects_prog.append(xprog)

    for xorg in qq1['data_project_org']:
        x_res_xlst_projects_org.append(xorg)

df1 = pd.DataFrame(x_res_xlst_projects)
df2 = pd.DataFrame(x_res_xlst_projects_prog)
df3 = pd.DataFrame(x_res_xlst_projects_org)

print '--proj: ', len(df1)
print '--prog: ', len(df2)
print '--orga: ', len(df3)

print 'parsing done --', time.ctime()

# save in sql
print 'start saving ', time.ctime(), len(df2)

df1.to_sql(name='eu_fp7_projects',
           con=xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_')

print 'saved - proj', time.ctime(), len(df1)

df2.to_sql(name='eu_fp7_projects_prog',
           con=xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved - prog', time.ctime(), len(df2)

df3.to_sql(name='eu_fp7_projects_org',
           con=xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_')

print 'saved - proj', time.ctime(), len(df3)

# # H2020


xzip_File = xFld_Path_set + 'cordis-h2020projects-xml.zip'

print 'start--', time.ctime()
xq1 = UT.read_zipped_file(xzip_File)
for xq2 in xq1:
    qq1 = ETP_EU.eu_h2020_getprojectdata(xq2)

# xq2

print 'done --', time.ctime()

# about 3 minutes to process only


x_res_xlst_projects = []
x_res_xlst_projects_prog = []
x_res_xlst_projects_org = []

xzip_File = xFld_Path_set + 'cordis-h2020projects-xml.zip'

print 'start--', time.ctime()
xq1 = UT.read_zipped_file(xzip_File)
for xnr, xq2 in enumerate(xq1):
    # for testing
    # if xnr == 10:
    #    break

    # process
    try:
        qq1 = ETP_EU.eu_h2020_getprojectdata(xq2)
        x_res_xlst_projects.append(qq1['data_project'])

        for xprog in qq1['data_project_prog']:
            x_res_xlst_projects_prog.append(xprog)

        for xorg in qq1['data_project_org']:
            x_res_xlst_projects_org.append(xorg)
    except:
        print 'error !!!!'

df1 = pd.DataFrame(x_res_xlst_projects)
df2 = pd.DataFrame(x_res_xlst_projects_prog)
df3 = pd.DataFrame(x_res_xlst_projects_org)

print '--proj: ', len(df1)
print '--prog: ', len(df2)
print '--orga: ', len(df3)

print 'parsing done --', time.ctime()

# save in sql
print 'start saving ', time.ctime(), len(df2)

df1.to_sql(name='eu_h2020_projects',
           con=xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved - proj', time.ctime(), len(df1)

df2.to_sql(name='eu_h2020_projects_prog',
           con=xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_')

print 'saved - prog', time.ctime(), len(df2)

df3.to_sql(name='eu_h2020_projects_org',
           con=xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved - org', time.ctime(), len(df3)
