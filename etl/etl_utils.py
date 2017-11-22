
# -*- coding: utf-8 -*-
"""
Created on Sat Nov 07 14:50:32 2015

@author: Mike

"""

'''
a set of functions for data acquisition from research funding agencies 
refer to the documentation folder for further information 
'''

import zipfile


def read_zipped_file(x_zip_file):
    '''
    open ziped file and read each file included
    '''
    with zipfile.ZipFile(x_zip_file, "r") as f:
        for x_file_name in f.namelist():
            x_data_read = f.read(x_file_name)
            yield x_data_read