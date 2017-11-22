

# coding: utf-8

# # parsing grant files   NSF

# add parse function
import sys
xPath_package = '/home/mike/PycharmProjects/grant_db/'
sys.path.append(xPath_package)

import pandas as pd
import os

#from etl import etl_utils as UT
from etl import etl_parse_nsf as P_NSF


# PATH to the parsed results
xFld_Path_parsed = '/media/mike/SSD_Data/__data_staging/1000_grant_db/2000_dset_parsed/'

xDB = xFld_Path_parsed + 'grant_data_parsed.db'
xDBCon = 'sqlite:///' + xDB


# PATH to datasets
xFld_Path_dset_original = '/media/mike/SSD_Data/__data_staging/1000_grant_db/1000_dset_original/'
xFld_Path_set = xFld_Path_dset_original + '5000_NSF/'

xlst_Files = os.listdir(xFld_Path_set)
xFile = xlst_Files[0]
xfile_name = xFld_Path_set  + xFile
xfile_name



xq1 = P_NSF.parse_nsf_grant_to_csv(xfile_name )

xq1.keys()

df1 = xq1['grant_info']
df1.head()
