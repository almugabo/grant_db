

# coding: utf-8

# # parsing grant files   NSF

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

# # NSF


from etl import etl_parse_nsf as p_nsf


xFld = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/5000_NSF/'


print '! starting', time.ctime()

for x_year in range(2000, 2017):
    x_year_str = str(x_year)
    xFile_Zipped = xFld + x_year_str + '.zip'
    print '--process', x_year_str

    xq1 = p_nsf.parse_nsf_grant_to_csv(xFile_Zipped)

    for xkey in xq1.keys():
        xTabName = 'nsf_' + x_year_str + '_' + xkey

        (xq1[xkey]).to_sql(name=xTabName,
                           con=xDBCon,
                           if_exists='replace',
                           index=True, index_label='record_id',
                           chunksize=10000)
        print '------saved', xTabName, 'records:', len(xq1[xkey]), time.ctime()

print '-- all done', time.ctime()
