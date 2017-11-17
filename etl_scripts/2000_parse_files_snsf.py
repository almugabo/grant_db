
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


# In[4]:

xFld_Path_parsed = '/media/mike/SSD_Data/__data_staging/1000_grant_db/2000_dset_parsed/'

xDB = xFld_Path_parsed + 'grant_data_parsed.db'
xDBCon = 'sqlite:///' + xDB


# # Swiss national Science foundation

# In[5]:

xFld_Path_dset_original = '/media/mike/SSD_Data/__data_staging/1000_grant_db/1000_dset_original/'
xFld_Path_set = xFld_Path_dset_original + '2000_SNSF/'

os.listdir(xFld_Path_set)


# In[13]:

# projects data

x_df1 = pd.read_csv(xFld_Path_set + 'P3_GrantExport.csv', sep = ';', encoding='utf-8')
x_df1.head()

x_df1.to_sql(name = 'snsf_projects',
             con = xDBCon, 
             if_exists='replace', 
             index=True, 
             index_label='record_id')
print '--projects'

#project data abstracts 
x_df1 = pd.read_csv(xFld_Path_set + 'P3_GrantExport_with_abstracts.csv', sep = ';' encoding='utf-8')
x_df1.to_sql(name = 'snsf_projects_abstracts',
             con = xDBCon, 
             if_exists='replace', 
             index=True, 
             index_label='record_id')

print '--projects with abstracts'

# person 
x_df1 = pd.read_csv(xFld_Path_set + 'P3_PersonExport.csv', sep = ';', encoding='utf-8')
x_df1.head()
print len(x_df1)
x_df1.to_sql(name = 'snsf_person',
             con = xDBCon, 
             if_exists='replace', 
             index=True, 
             index_label='record_id')

print 'OK--person'


# collaborations 
x_df1 = pd.read_csv(xFld_Path_set + 'P3_CollaborationExport.csv', sep = ';', encoding='utf-8')
print len(x_df1)
x_df1.head()

x_df1.to_sql(name = 'snsf_collaborations',
             con = xDBCon, 
             if_exists='replace', 
             index=True, 
             index_label='record_id')

print 'OK--collaborations'


x_df1 = pd.read_csv(xFld_Path_set + 'P3_GrantOutputDataExport.csv', sep = ';', encoding='utf-8')
print len(x_df1)
x_df1.head()


x_df1.to_sql(name = 'snsf_grant_output',
             con = xDBCon, 
             if_exists='replace', 
             index=True, 
             index_label='record_id')

print 'OK--grant_output'



# In[ ]:




# In[ ]:



