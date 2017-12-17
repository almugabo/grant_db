# coding: utf-8

# # Testing parsing of various datasets

# In[1]:

import time
import sys
from sqlalchemy import create_engine
import zipfile
import os
import pandas as pd

# add parse function
xPath_package = '/home/mike/PycharmProjects/grant_db/'
sys.path.append(xPath_package)

# connection strings
# xPGConnString = 'postgresql://postgres:post@localhost:5432/eris'
# xDBCon = create_engine(xPGConnString)

xDB = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/2000_dset_parsed/grant_dset.db'
xDBCon = 'sqlite:///' + xDB

xFld = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/4000_NIH/_CSV/'

# In[19]:

# IMPORT THE TABLES -- projects
print '--- starting', time.ctime()
for x_year in range(2006, 2017):
    x_year_str = str(x_year)
    print '-------------------- process: ', x_year_str
    xFile_Zipped = xFld + x_year_str + '.zip'
    xFile_nonZipped = 'RePORTER_PRJ_C_FY' + x_year_str + '.csv'
    xTabName = 'nih_' + x_year_str + 'projects'

    # Process File
    # df1 = None
    with zipfile.ZipFile(xFile_Zipped) as z:
        with z.open(xFile_nonZipped) as f:
            df1 = pd.read_csv(f, low_memory=False)
            # print len(df1)

            df1.to_sql(name=xTabName,
                       con=xDBCon,
                       if_exists='replace',
                       index=True, index_label='record_id',
                       chunksize=10000)

            print 'saved ! records:', len(df1), time.ctime()

print '--------all done---', time.ctime()



