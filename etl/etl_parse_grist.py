'''
parsing grist files
'''

import json


def grist_parse_file(xFileName):
    '''
    parse grist file to get data on grant, organisation and grant
    returns a dictionary with
    keys 'xData_Pers', 'xData_Inst', 'xData_Grant'
    N.B:
    for now assumed 1:1
    may not be correct and need changes
    '''
    # result Dictionary
    xDict_Res = dict.fromkeys(['xData_Pers', 'xData_Inst', 'xData_Grant'])

    with open(xFileName, 'r') as ff:
        xData = ff.read()
        xDataJson = json.loads(xData)

        # Grant Data
    xDataDict_Grant = {}

    xDataDict_Grant['Funder'] = xDataJson['Grant']['Funder']
    xDataDict_Grant['FundRefID'] = xDataJson['Grant']['FundRefID']

    xDataDict_Grant['Id'] = xDataJson['Grant']['Id']
    if xDataJson['Grant'].has_key('Alias'):
        xDataDict_Grant['Alias'] = xDataJson['Grant']['Alias']
    xDataDict_Grant['Title'] = xDataJson['Grant']['Title']

    if xDataJson['Grant'].has_key('Abstract'):
        if isinstance(xDataJson['Grant']['Abstract'], dict):
            x_lst_abstr = [xDataJson['Grant']['Abstract']]
        else:
            x_lst_abstr = xDataJson['Grant']['Abstract']

        # get only english, scientific abstracts
        for xAbst in x_lst_abstr:
            if (xAbst['@Language'] == 'en') and (xAbst['@Type'] == 'scientific'):
                xDataDict_Grant['Abstract'] = xAbst['$']
    # xDataDict_Grant
    if (xDataJson['Grant']).has_key('Type'):
        xDataDict_Grant['Type'] = xDataJson['Grant']['Type']
    if (xDataJson['Grant']).has_key('Stream'):
        xDataDict_Grant['Stream'] = xDataJson['Grant']['Stream']
    if (xDataJson['Grant']).has_key('StartDate'):
        xDataDict_Grant['StartDate'] = xDataJson['Grant']['StartDate']
    if (xDataJson['Grant']).has_key('EndDate'):
        xDataDict_Grant['EndDate'] = xDataJson['Grant']['EndDate']
    if (xDataJson['Grant']).has_key('Amount'):
        xDataDict_Grant['Amount'] = xDataJson['Grant']['Amount']['$']
        xDataDict_Grant['Amount_Currency'] = xDataJson['Grant']['Amount']['@Currency']

    xDict_Res['xData_Grant'] = xDataDict_Grant


    # Person Data
    xDataDict_Person = {}
    xDataDict_Person['FundRefID'] = xDataJson['Grant']['FundRefID']
    xDataDict_Person['Grant_Id']  = xDataJson['Grant']['Id']

    xDataDict_Person['FamilyName'] = xDataJson['Person']['FamilyName']
    if (xDataJson['Person']).has_key('GivenName'):
        xDataDict_Person['GivenName'] = xDataJson['Person']['GivenName']
    if (xDataJson['Person']).has_key('Initials'):
        xDataDict_Person['Initials'] = xDataJson['Person']['Initials']
    if (xDataJson['Person']).has_key('Title'):
        xDataDict_Person['Title'] = xDataJson['Person']['Title']
    if (xDataJson['Person']).has_key('Alias'):
        if isinstance(xDataJson['Person']['Alias'], dict):
            x_lst = [xDataJson['Person']['Alias']]
        else:
            x_lst = xDataJson['Person']['Alias']
        xDataDict_Person['Alias'] = ';'.join([x['@Source'] + '_' + x['$'] for x in x_lst])

    xDict_Res['xData_Pers'] = xDataDict_Person

    # Institution
    # xDataJson['Institution']


    if xDataJson.has_key('Institution'):
        xDataDict_Inst = {}
        xDataDict_Inst['FundRefID'] = xDataJson['Grant']['FundRefID']
        xDataDict_Inst['Grant_Id'] = xDataJson['Grant']['Id']


        xDataDict_Inst['InstName'] = xDataJson['Institution']['Name']
        if (xDataJson['Institution']).has_key('Department'):
            xDataDict_Inst['InstDepartment'] = xDataJson['Institution']['Department']

        xDict_Res['xData_Inst'] = xDataDict_Inst




    return xDict_Res
