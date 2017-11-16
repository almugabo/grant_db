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


def gtr_project_data(x_destination_folder):
    '''
    bulk retrieve data from Gateway to research

    '''
    xUrlBase = 'http://gtr.rcuk.ac.uk/gtr/api/projects.json?p={0}&s=20'
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
    xFileName = x_destination_folder + xResultPage + '_' + x_time_stamp
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
            xFileName = x_destination_folder + xResultPage + '_' + x_time_stamp
            with open(xFileName, 'w') as f:
                json.dump(q2, f)
        except:
            print 'error on page', xPageNr

    print 'GTR projects data fetched and saved !'
