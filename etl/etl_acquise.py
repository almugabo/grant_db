# -*- coding: utf-8 -*-
"""
Created on Sat Nov 07 14:50:32 2015

@author: Mike
"""

'''
utility functions for data acquisition 
'''

import requests


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
    return x_filename_local
