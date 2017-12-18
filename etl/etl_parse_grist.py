
def grist_grant_data (xFileName):
    '''
    open single grist file and get grant data
    '''

    xDictRes = {}

    xDataDict_Grant = {}
    xDataDict_Person = {}
    xDataDict_Inst = {}


    with open(xFileName, 'r') as ff:
        xData = ff.read()
        xDataJson = json.loads(xData)

    # Grant Data
    xDataDict_Grant['Funder']    = xDataJson['Grant']['Funder']
    xDataDict_Grant['FundRefID'] = xDataJson['Grant']['FundRefID']
    xDataDict_Grant['GrantId']        = xDataJson['Grant']['Id']
    xDataDict_Grant['Title']     = xDataJson['Grant']['Title']
    # xDataDict
    if (xDataJson['Grant']).has_key('Type'):
        xDataDict_Grant['Type']      = xDataJson['Grant']['Type']
    if (xDataJson['Grant']).has_key('Stream'):
        xDataDict_Grant['Stream']    = xDataJson['Grant']['Stream']
    if (xDataJson['Grant']).has_key('StartDate'):
        xDataDict_Grant['StartDate'] = xDataJson['Grant']['StartDate']
    if (xDataJson['Grant']).has_key('EndDate'):
        xDataDict_Grant['EndDate']   = xDataJson['Grant']['EndDate']
    if (xDataJson['Grant']).has_key('Amount'):
        xDataDict_Grant['Amount']    = xDataJson['Grant']['Amount']['$']
        xDataDict_Grant['Amount_Currency'] = xDataJson['Grant']['Amount']['@Currency']

    # -- if xDataJson.has_key('Alias'):
    # --    xDataDict_Grant['Alias'] = xDataJson['Alias']

    if (xDataJson['Grant']).has_key('Abstract'):
        if isinstance(xDataJson['Grant']['Abstract'], dict):
            x_lst_abstr = [xDataJson['Grant']['Abstract']]
        else:
            x_lst_abstr = xDataJson['Grant']['Abstract']

        # get only english, scientific abstracts
        for xAbst in x_lst_abstr:
            if (xAbst['@Language'] == 'en') and (xAbst['@Type'] == 'scientific'):
                xDataDict_Grant['Abstract_scientific'] = xAbst['$']
            if (xAbst['@Language'] == 'en') and (xAbst['@Type'] == 'lay'):
                xDataDict_Grant['Abstract_lay'] = xAbst['$']

    # Person Data
    # -- add Grant Data
    xDataDict_Person['FundRefID'] = xDataDict_Grant['FundRefID']
    xDataDict_Person['GrantId'] = xDataDict_Grant['GrantId']

    xDataDict_Person['FamilyName'] = xDataJson['Person']['FamilyName']
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

    # Institution
    # -- add grant Data
    xDataDict_Inst['FundRefID'] = xDataDict_Grant['FundRefID']
    xDataDict_Inst['GrantId'] = xDataDict_Grant['GrantId']

    xDataDict_Inst['InstName'] = xDataJson['Institution']['Name']
    if (xDataJson['Institution']).has_key('Department'):
        xDataDict_Inst['InstDepartment'] = xDataJson['Department']

    xDictRes['data_grant'] = xDataDict_Grant
    xDictRes['data_person'] = xDataDict_Person
    xDictRes['data_institution'] = xDataDict_Inst

    return xDictRes

