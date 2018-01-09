
# coding: utf-8

# # a set of scripts to parse the downloaded files
#
# The approach here is to load the data in an sqlite database for further processing
#
# each dataset has its own structure and need to processed separately.
#

# In[3]:

import pandas as pd
#import os
#import sys
#import time
#from sqlalchemy import create_engine



# # Croatian Science Foundation

# In[22]:

xFld_Path_dset_original = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/'
xFld_Path_set = xFld_Path_dset_original + '3000_HRZZ/'


xFld_Path_parsed = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/2000_dset_parsed/'
xDB = xFld_Path_parsed + 'grant_dset.db'
xDBCon = 'sqlite:///' + xDB


# In[27]:

# projects data
x_df1 = pd.read_excel(xFld_Path_set + 'hrrzz_2029062017.xlsx')
print len(x_df1)

x_df1.to_sql(name = 'hrrzz_projects',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'OK--HRZZ projects saved', len(x_df1)


