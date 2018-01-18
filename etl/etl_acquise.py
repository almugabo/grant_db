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
import math


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


def grist_getGrantData(xOrg_Id, xDestFolder):
    '''
    bulk retrieve data from GRIST API

    :param xOrg_Id:
    :param xDestFolder:
    :return:
    '''
    def getGrant_Id(x_record):
        x_grant = x_record['Grant']
        x_grant_id = x_grant['Id']
        x_grant_id_org = ((x_grant['FundRefID']).split('/'))[4]
        return str(x_grant_id_org), str(x_grant_id)

    xUrl_1 = 'http://www.ebi.ac.uk/europepmc/GristAPI/rest/get/query=grant_agency:'
    xUrl_2 = '&resultType=core&format=json'
    xUrl_3 = '&page='
    # first request
    xPage = 1
    xUrl = xUrl_1 + xOrg_Id + xUrl_2 + xUrl_3 + str(xPage)

    q1 = requests.get(xUrl)
    q2 = json.loads(q1.content)
    if q2['RecordList']:
        x_records = q2['RecordList']['Record']  # list
        xrecords_nr = int(q2['HitCount'])
        # remaining pages
        xBuckets_nr = int(math.ceil(xrecords_nr / 25.0))
        xBuckets_pages = [1 + x for x in range(xBuckets_nr)][1:]

        for x_record in x_records:
            xrec_id = getGrant_Id(x_record)
            xFileName = xDestFolder + xrec_id[0] + '_' + xrec_id[1] + '.json'

            with open(xFileName, 'w') as f:
                json.dump(x_record, f)

                # remaining
            xPage = 1
    xUrl = xUrl_1 + xOrg_Id + xUrl_2 + xUrl_3 + str(xPage)

    q1 = requests.get(xUrl)
    q2 = json.loads(q1.content)
    if q2['RecordList']:
        x_records = q2['RecordList']['Record']  # list
        xrecords_nr = int(q2['HitCount'])
        # remaining pages
        xBuckets_nr = int(math.ceil(xrecords_nr / 25.0))
        xBuckets_pages = [1 + x for x in range(xBuckets_nr)][1:]

        for x_record in x_records:
            xrec_id = getGrant_Id(x_record)
            xFileName = xDestFolder + xrec_id[0] + '_' + xrec_id[1] + '.json'

            with open(xFileName, 'w') as f:
                json.dump(x_record, f)

    print 'done'




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
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/projects.json?p={0}&s=100'
    if entities == 'organisations':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/organisations.json?p={0}&s=100'
    if entities == 'persons':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/persons.json?p={0}&s=100'
    if entities == 'publications':
        xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/outcomes/publications.json?p=1&s=100'
    if entities == 'ipr':
        xUrlBase = 'http://gtr.rcuk.ac.uk:80/gtr/api/outcomes/intellectualproperties.json?p=1&s=100'

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




