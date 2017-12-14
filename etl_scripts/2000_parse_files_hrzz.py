
# coding: utf-8

# # a set of scripts to parse the downloaded files
# 
# The approach here is to load the data in an sqlite database for further processing 
# 
# each dataset has its own structure and need to processed separately. 
# 

# In[3]:

import pandas as pd 
import os
import sys
import time
from sqlalchemy import create_engine

# In[4]:


xPGConnString = 'postgresql://postgres:post@localhost:5432/x_eris'
xDBCon = create_engine(xPGConnString)


# # Croatian Science Foundation

# In[22]:

xFld_Path_dset_original = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/'
xFld_Path_set = xFld_Path_dset_original + '3000_HRZZ/'

os.listdir(xFld_Path_set)


# In[27]:

# projects data
x_df1 = pd.read_excel(xFld_Path_set + 'hrrzz_2029062017.xlsx')
print len(x_df1)

x_df1.to_sql(name = 'hrrzz_projects',
             con = xDBCon, 
             if_exists='replace', 
             index=True, 
             index_label='record_id',
             schema='data_staging')

print 'OK--HRZZ projects saved', len(x_df1)




# In[25]:



