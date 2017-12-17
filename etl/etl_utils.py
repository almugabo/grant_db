
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
import time
import psycopg2
# important to deal with json especially for copy FROM DB
# import psycopg2.extras
#import psycopg2.extras
from string import Template


def read_zipped_file(x_zip_file):
    '''
    open ziped file and read each file included
    '''
    with zipfile.ZipFile(x_zip_file, "r") as f:
        for x_file_name in f.namelist():
            x_data_read = f.read(x_file_name)
            yield x_data_read





class Timer:
    def __enter__(self):
        self.start = time.clock()
        return self

    def __exit__(self, *args):
        self.end = time.clock()
        self.interval = self.end - self.start


class pg_copy:
    def __init__(self, xdb_name, xdb_host, xdb_port, xdb_user, xdb_passwd):
        '''
        set parameters for daabase connection
        example:
        xPGCopy = pg_copy(xdb_name= 'scopus',
                  xdb_host= 'localhost',
                  xdb_port= '5432',
                  xdb_user= 'postgres',
                  xdb_passwd = 'post')

        '''
        self.database = xdb_name
        self.host = xdb_host
        self.port = xdb_port
        self.user = xdb_user
        self.password = xdb_passwd

    def copy_to_textfile(self, xSQLQuery, xDestFile, xHeader=True):
        '''
        copy an table from an sql query to a textfile
        '''
        xdbconn_parameters = {'database': self.database,
                              'host': self.host,
                              'port': self.port,
                              'user': self.user,
                              'password': self.password
                              }

        ## connect to DB
        xConn = psycopg2.connect(database=xdbconn_parameters['database'],
                                 user=xdbconn_parameters['user'],
                                 password=xdbconn_parameters['password'],
                                 host=xdbconn_parameters['host'],
                                 port=xdbconn_parameters['port'])
        xCur = xConn.cursor()

        if xHeader:
            xQuery_output = "COPY ({0}) TO STDOUT DELIMITER '\t' CSV HEADER".format(xSQLQuery)
        else:
            xQuery_output = "COPY ({0}) TO STDOUT DELIMITER '\t' CSV".format(xSQLQuery)

        with Timer() as t:
            with open(xDestFile, 'w') as f:
                xCur.copy_expert(xQuery_output, f)

        print 'copying done in %.03f ' % t.interval

        xConn.close()

    def copy_from_file(self, xInputFile, xDestTable, xHeader=True, xSep='\t'):

        xdbconn_parameters = {'database': self.database,
                              'host': self.host,
                              'port': self.port,
                              'user': self.user,
                              'password': self.password
                              }

        ## connect to DB
        xConn = psycopg2.connect(database=xdbconn_parameters['database'],
                                 user=xdbconn_parameters['user'],
                                 password=xdbconn_parameters['password'],
                                 host=xdbconn_parameters['host'],
                                 port=xdbconn_parameters['port'])
        xCur = xConn.cursor()

        # Prepare SQL statemenet for copy expert
        if xHeader:
            xtpl = Template("COPY $xTable FROM STDIN WITH CSV HEADER DELIMITER AS '$xDelimiter'")
        else:
            xtpl = Template("COPY $xTable FROM STDIN WITH CSV DELIMITER AS '$xDelimiter'")

        xSQL_STATEMENT = xtpl.substitute(xTable=xDestTable, xDelimiter=xSep)

        # execute the copy operation
        with Timer() as t:
            with open(xInputFile, 'r') as f:
                xCur.copy_expert(sql=xSQL_STATEMENT, file=f)
        xConn.commit()

        print('Operation took %.03f sec.' % t.interval)

        xConn.close()
