
# coding: utf-8

# a set of scripts to parse the downloaded files and save them in an sqlite database for further processing
#  AUSTRALIAN RESEARCH COUNCIL


import pandas as pd
#import os
#from sqlalchemy import create_engine



#Set paths

xFld_Path_parsed = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/2000_dset_parsed/'


xDB = xFld_Path_parsed + 'grant_dset.db'
xDBCon = 'sqlite:///' + xDB

xFld_Path_dset_original = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/1000_dset_original/'
xFld_Path_arc = xFld_Path_dset_original + '1000_ARC/'

# -- completed - projects
x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_Projects_and_fellowships_completed.xlsx',
                      sheetname='Projects')

print len(x_df1)


x_df1 = x_df1[x_df1.columns[:-4]] # clean headers
x_df1.to_sql(name = 'arc_projects_completed',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id'
             )

print 'table', 'arc_projects_completed', 'saved'


# -- completed - fellowships
x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_Projects_and_fellowships_completed.xlsx',
                      sheetname='Fellowships')


x_df1.to_sql(name = 'arc_fellowships_completed',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'table', 'arc_fellowships_completed', 'saved'

# -- on-going - projects
x_df1 = pd.read_excel(io = xFld_Path_arc + 'NCGP_Projects_and_fellowship_new_and_ongoing.xlsx',
                      sheetname='Projects')

x_df1.to_sql(name = 'arc_projects_ongoing',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'table', 'arc_projects_ongoing', 'saved'


# -- on-going - fellowships

x_df1 = pd.read_excel(io = xFld_Path_arc + 'NCGP_Projects_and_fellowship_new_and_ongoing.xlsx',
                      sheetname='Fellowships')

x_df1.to_sql(name = 'arc_fellowships_ongoing',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'table', 'arc_fellowships_ongoing', 'saved'


# keywords - completed projects

x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_Keywords_completed_Feb2015.xlsx',
                      sheetname='Project Keywords')

x_df1.to_sql(name = 'arc_projects_keywords_completed',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id'
             )

print 'table', 'arc_projects_keywords_completed', 'saved'

# keywords - on going projects

x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_Keywords_new_and_ongoing_Feb2015.xlsx',
                      sheetname='Project Keywords')

x_df1.to_sql(name = 'arc_projects_keywords_ongoing',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'table', 'arc_projects_keywords_ongoing', 'saved'


# fields of research
x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_Field-of-Research_completed.xlsx',
                      sheetname='Project Classification')

x_df1.to_sql(name = 'arc_field_of_research_completed',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id'
             )

print 'table', 'arc_field_of_research_completed', 'saved'



x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_FoR_new_and_ongoing.xlsx',
                      sheetname='Project Classification')

x_df1.to_sql(name = 'arc_field_of_research_on_going',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'table', 'arc_field_of_research_on_going', 'saved'


# partners

x_df1 = pd.read_excel(io = xFld_Path_arc + 'ARC_NCGP_PartnerOrgs.xlsx',
                      sheetname='Partner Organisations')

x_df1.to_sql(name = 'arc_projects_organisations',
             con = xDBCon,
             if_exists='replace',
             index=True,
             index_label='record_id')

print 'table', 'arc_projects_organisations', 'saved'

print 'all table saved'

