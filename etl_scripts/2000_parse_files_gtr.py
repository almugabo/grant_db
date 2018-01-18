


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

for xFileData in xlst_data[0:100]:
    xFileName = xFld_GTR_Data + xFileData

    xlst_dict_persons = xlst_dict_persons + GTR_PAR.gtr_parse_person(xFileName)

print 'persons records:', len(xlst_dict_persons)

x_df = pd.DataFrame(xlst_dict_persons[0:6])

x_df.to_sql(name = 'gtr_persons',
           con = xDBCon,
           if_exists='replace',
           index=True,
           index_label='record_id_'
           )

print 'saved, persons', time.ctime()




