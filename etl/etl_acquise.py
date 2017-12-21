# -*- coding: utf-8 -*-
"""
Created on Sat Nov 07 14:50:32 2015

@author: Mike

"""

'''
a set of functions for data acquisition from research funding agencies 
refer to the documentation folder for further information 
'''

import requests
import json
import time
import datetime


def download_file(x_url, x_filename_local):
    ''' download files,
    e.g. zip files of XML

    TO DO : add proxy options
    '''
    # NOTE the stream=True parameter
    r = requests.get(x_url, stream=True)
    with open(x_filename_local, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
                f.flush()
    # return x_filename_local


import requests
import json
import time
import datetime


def gtr_get_data(xDestFolder, entities='projects'):
    '''
    bulk retrieve data from Gateway to research
    various entities can be retrieved
    entities can be
    - 'projects',
    - 'organisations',
    - 'persons',
    - 'publications',
    - 'ipr'

    There are other interesting data which are not considered now:
    such as spinouts, impact summaries etc ...

    Exact Url specifications can be found here
    http://gtr.rcuk.ac.uk/gtr/api/examples

    QUESTION DO WE NEED FUNDERS AS WELL ?
    http://gtr.rcuk.ac.uk/gtr/api/funds.json?p=1&s=20

    '''
    # specify the enrities to be retrieved

    if entities == 'projects':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/projects.json?p={0}&s=20'
    if entities == 'organisations':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/organisations.json?p={0}&s=20'
    if entities == 'persons':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/persons.json?p={0}&s=20'
    if entities == 'publications':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/outcomes/publications.json?p=1&s=20'
    if entities == 'ipr':
        xUrlBase = 'http://gtr.rcuk.ac.uk:80/gtr/api/outcomes/intellectualproperties?p=1&s=20'

    # Page 1
    xPageNr = str(1)
    xUrl = xUrlBase.format(xPageNr)
    q1 = requests.get(xUrl)
    q2 = json.loads(q1.content)
    xResultPage = str(q2['page'])
    # get total page number for remaining pages
    xTotalPageNr = q2['totalPages']
    xPageRange = [str(x) for x in range(1, xTotalPageNr)[1:]]
    # save page 1
    time.sleep(1)
    x_time_stamp = str(datetime.datetime.now()).replace('-', '_').replace(':', '_').replace('.', '_').replace(' ', '_')
    xFileName = xDestFolder + xResultPage + '_' + x_time_stamp
    with open(xFileName, 'w') as f:
        json.dump(q2, f)

    # remaining pages
    for xPageNr in xPageRange:
        try:
            xUrl = xUrlBase.format(xPageNr)
            time.sleep(1)
            q1 = requests.get(xUrl)
            q2 = json.loads(q1.content)
            xResultPage = str(q2['page'])

            x_time_stamp = str(datetime.datetime.now()).replace('-', '_').replace(':', '_').replace('.', '_').replace(
                ' ', '_')
            xFileName = xDestFolder + xResultPage + '_' + x_time_stamp
            with open(xFileName, 'w') as f:
                json.dump(q2, f)
        except:
            print
            'error on page', xPageNr

    print
    'GTR projects data fetched and saved !'


# In[ ]:


# In[ ]:

import json


def gtr_parse_projects(xFile):
    '''
    parse project data from Gateway to research
    and return a dictionary with project data and a list of "relatioons"
    N.B: metadata on relations such as PI, Funding Organisation or Performing Organisation
    should be fetched separately
    for example given their IDs
    '''

    xDictRes = {}

    with open(xFile, 'r') as ff:
        xData = ff.read()
    xDataJson = json.loads(xData)

    xFields_project = ['id',
                       'created',
                       'href',
                       'status',
                       'title',
                       'grantCategory',
                       'abstractText',
                       'potentialImpact',
                       'identifiers',
                       'researchTopics',
                       'researchSubjects',
                       'healthCategories',
                       'researchActivities',
                       'leadOrganisationDepartment'
                       ]

    # links
    xFields_links = ['id', 'href', 'rel']
    xlst_links_res = []  # results links

    x_data_project = dict.fromkeys(xFields_project)
    x_data_project['id'] = x_project['id']
    x_data_project['created'] = x_project['created']

    x_data_project['href'] = x_project['href']
    x_data_project['status'] = x_project['status']
    x_data_project['title'] = x_project['title']
    x_data_project['grantCategory'] = x_project['grantCategory']

    x_data_project['abstractText'] = x_project['abstractText']

    if x_project.has_key('potentialImpact'):
        x_data_project['potentialImpact'] = x_project['potentialImpact']

    if x_project.has_key('identifiers'):
        lst_identifiers = x_project['identifiers']['identifier']
        x_data_project['identifiers'] = ';'.join([x['type'] + '_' + x['value'] for x in lst_identifiers])

    if x_project.has_key('researchTopics'):
        xlst_research_topics = x_project['researchTopics']['researchTopic']
        x_data_project['researchTopics'] = ';'.join([x['id'] + '_' + x['text'] for x in xlst_research_topics])

    if x_project.has_key('researchSubjects'):
        xlst_research_topics = x_project['researchSubjects']['researchSubject']
        x_data_project['researchSubjects'] = ';'.join([x['id'] + '_' + x['text'] for x in xlst_research_topics])

    if x_project.has_key('healthCategories'):
        xlst_research_topics = x_project['healthCategories']['healthCategory']
        x_data_project['healthCategories'] = ';'.join([x['id'] + '_' + x['text'] for x in xlst_research_topics])

    if x_project.has_key('researchActivities'):
        xlst_research_topics = x_project['researchActivities']['researchActivity']
        x_data_project['researchActivities'] = ';'.join([x['id'] + '_' + x['text'] for x in xlst_research_topics])

    if x_project.has_key('leadOrganisationDepartment'):
        x_data_project['leadOrganisationDepartment'] = x_project['leadOrganisationDepartment']

    # process links
    xlst_links = x_project['links']['link']

    for xDict in xlst_links:
        xDictLink = dict.fromkeys(xFields_links)
        xDictLink['id'] = x_project['id']
        xDictLink['href'] = xDict['href']
        xDictLink['rel'] = xDict['rel']
        xlst_links_res.append(xDictLink)

    xDictRes['grant_data'] = x_data_project
    xDictRes['grant_links'] = xlst_links_res


return xDictRes