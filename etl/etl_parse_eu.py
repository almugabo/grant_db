'''
parsing grant data from the European Union
see datasets_source for more information on the datasets
'''

import xmltodict


xFields_project_fp7 = ['source', 'language', 'rcn', 'reference', 'acronym', 'teaser', 'title', 'objective', 'totalCost',
                       'ecMaxContribution', 'contentCreationDate', 'contentUpdateDate',
                       'lastUpdateDate', 'contract_startDate', 'contract_endDate', 'contract_duration',
                       'call_type', 'call_rcn', 'call_title', 'call_identifier']

xFields_project_programme_fp7 = ['project_rcn', 'programme_type', 'programme_rcn', 'programme_code',
                                 'programme_pga', 'programme_title', 'programme_shortTitle', 'programme_url']

xFields_project_organisation_fp7 = ['project_rcn', 'org_order', 'org_type', 'org_rcn',
                                    'org_legalName', 'org_shortName', 'org_departmentName',
                                    'org_adress_street', 'org_adress_city', 'org_adress_postalCode',
                                    'org_adress_country', 'org_adress_geolocation'
                                    ]

xFields_project_h2020 = ['source', 'language', 'rcn', 'reference', 'acronym', 'teaser', 'title', 'objective',
                         'totalCost',
                         'ecMaxContribution', 'endDate', 'startDate', 'contentCreationDate', 'contentUpdateDate',
                         'lastUpdateDate', 'contract_duration',
                         'call_type', 'call_rcn', 'call_title', 'call_identifier']

xFields_project_programme_h2020 = ['project_rcn', 'programme_type', 'programme_rcn', 'programme_code',
                                   'programme_frameworkProgramme', 'programme_title', 'programme_shortTitle']

xFields_project_organisation_h2020 = ['project_rcn', 'org_order', 'org_type', 'org_rcn',
                                      'org_legalName', 'org_shortName', 'org_departmentName',
                                      'org_adress_street', 'org_adress_city', 'org_adress_postalCode',
                                      'org_adress_country',
                                      'org_id',
                                      'org_ecContribution', 'org_terminated']
def eu_fp7_getprojectdata(xData):
    '''
     take data from XML file
     xData : data read from the file as in
     with open(xFile_h2020, 'r') as f:
        xData = f.read()

     one project per file
     data 3 categories of data
     (1) project data
     (2) organisation data
     (3) programme data
     N.B: data on the call for proposal are in the project data
     N.B: hierachy of the programmes should be processed separately and not at level of each project

     The function returns a dictionary with
     'data_project' : data on a project (one to one)
     'data_project_prog' : a list of dictionaries with data on programmes/subprogrammes
     'data_project_org': a list of dictionaries with data organisations

    '''

    # Fields needed

    xresult_dict = dict.fromkeys(['data_project', 'data_project_prog', 'data_project_org'])
    xproject_lst_prog = []
    xproject_lst_org = []

    # project data

    xproject_dict = dict.fromkeys(xFields_project_fp7)

    q1 = xmltodict.parse(xData)
    q2 = q1['project']

    xproject_dict['source'] = q2['@xmlns']
    xproject_dict['language'] = q2['language']
    #xproject_dict['availableLanguages'] = q2['availableLanguages']
    xproject_dict['rcn'] = q2['rcn']
    xproject_dict['reference'] = q2['reference']
    xproject_dict['acronym'] = q2['acronym']
    xproject_dict['teaser'] = q2['teaser']

    xproject_dict['title'] = q2['title']
    xproject_dict['objective'] = q2['objective']
    xproject_dict['totalCost'] = q2['totalCost']
    xproject_dict['ecMaxContribution'] = q2['ecMaxContribution']

    #xproject_dict['endDate'] = q2['startDate']
    #xproject_dict['startDate'] = q2['endDate']

    xproject_dict['contentCreationDate'] = q2['contentCreationDate']
    xproject_dict['contentUpdateDate'] = q2['contentUpdateDate']
    xproject_dict['lastUpdateDate'] = q2['lastUpdateDate']
    if q2.has_key('contract'):
        xproject_dict['contract_startDate'] = q2['contract']['startDate']
        xproject_dict['contract_endDate'] = q2['contract']['endDate']
        if q2['contract'].has_key('duration'):
            xproject_dict['contract_duration'] = q2['contract']['duration']

    xproject_dict['call_type'] = q2['relations']['associations']['call']['@type']
    xproject_dict['call_rcn'] = q2['relations']['associations']['call']['rcn']
    xproject_dict['call_title'] = q2['relations']['associations']['call']['title']
    xproject_dict['call_identifier'] = q2['relations']['associations']['call']['identifier']

    # programmes
    x_progs = q2['relations']['associations']['programme']

    for x_prog in x_progs:
        xproject_dict_prog = dict.fromkeys(xFields_project_programme_fp7)

        xproject_dict_prog['project_rcn'] = q2['rcn']
        xproject_dict_prog['programme_type'] = x_prog['@type']
        xproject_dict_prog['programme_rcn'] = x_prog['rcn']
        xproject_dict_prog['programme_code'] = x_prog['code']
        xproject_dict_prog['programme_title'] = x_prog['title']

        # some info exists only for related programmes (and not sub programmes )
        if x_prog.has_key('pga'):
            xproject_dict_prog['programme_pga'] = x_prog['pga']
        if x_prog.has_key('shortTitle'):
            xproject_dict_prog['programme_shortTitle'] = x_prog['shortTitle']
        if x_prog.has_key('url'):
            xproject_dict_prog['programme_url'] = x_prog['url']

        xproject_lst_prog.append(xproject_dict_prog)

    # organisations
    x_orgs = q2['relations']['associations']['organization']
    # in some cases you have one organisations and it is not a list
    if type(x_orgs) != list:
        x_orgs = [x_orgs]

    for x_org in x_orgs:
        xproject_dict_org = dict.fromkeys(xFields_project_organisation_fp7)
        xproject_dict_org['project_rcn'] = q2['rcn']
        xproject_dict_org['org_order'] = x_org['@order']
        xproject_dict_org['org_type'] = x_org['@type']
        xproject_dict_org['org_rcn'] = x_org['rcn']

        if x_org.has_key('legalName'):
            xproject_dict_org['org_legalName'] = x_org['legalName']
        if x_org.has_key('shortName'):
            xproject_dict_org['org_shortName'] = x_org['shortName']
        if x_org.has_key('departmentName'):
            xproject_dict_org['org_departmentName'] = x_org['departmentName']
            #there were some cases in which the departments were a list
            if type(x_org['departmentName']) == list:
                xproject_dict_org['org_departmentName'] = ';'.join(xproject_dict_org['org_departmentName'])

        if x_org.has_key('address'):

            if x_org['address'].has_key('street'):
                xproject_dict_org['org_adress_street'] = x_org['address']['street']

            if x_org['address'].has_key('city'):
                xproject_dict_org['org_adress_city'] = x_org['address']['city']

            if x_org['address'].has_key('postalCode'):
                xproject_dict_org['org_adress_postalCode'] = x_org['address']['postalCode']
            if x_org['address'].has_key('country'):
                xproject_dict_org['org_adress_country'] = x_org['address']['country']
            if x_org['address'].has_key('geolocation'):
                xproject_dict_org['org_adress_geolocation'] = x_org['address']['geolocation']

        xproject_lst_org.append(xproject_dict_org)

    xresult_dict['data_project'] = xproject_dict
    xresult_dict['data_project_prog'] = xproject_lst_prog
    xresult_dict['data_project_org'] = xproject_lst_org

    return xresult_dict


def eu_h2020_getprojectdata(xData):
    xresult_dict = dict.fromkeys(['data_project', 'data_project_prog', 'data_project_org'])
    xproject_lst_prog = []
    xproject_lst_org = []

    # project data

    xproject_dict = dict.fromkeys(xFields_project_h2020)

    q1 = xmltodict.parse(xData)
    if q1.has_key('project') == False:
        return None

    q2 = q1['project']

    xproject_dict['source'] = q2['@xmlns']
    xproject_dict['language'] = q2['language']
    #xproject_dict['availableLanguages'] = q2['availableLanguages']
    xproject_dict['rcn'] = q2['rcn']
    xproject_dict['reference'] = q2['reference']
    xproject_dict['acronym'] = q2['acronym']
    xproject_dict['teaser'] = q2['teaser']

    xproject_dict['title'] = q2['title']
    xproject_dict['objective'] = q2['objective']
    if q2.has_key('totalCost'):
        xproject_dict['totalCost'] = q2['totalCost']
    if q2.has_key('ecMaxContribution'):
        xproject_dict['ecMaxContribution'] = q2['ecMaxContribution']

    if q2.has_key('startDate'):
        xproject_dict['endDate'] = q2['startDate']
    if q2.has_key('endDate'):
        xproject_dict['startDate'] = q2['endDate']

    xproject_dict['contentCreationDate'] = q2['contentCreationDate']
    xproject_dict['contentUpdateDate'] = q2['contentUpdateDate']
    xproject_dict['lastUpdateDate'] = q2['lastUpdateDate']

    xproject_dict['contract_duration'] = q2['contract']['duration']

    xproject_dict['call_type'] = q2['relations']['associations']['call']['@type']
    xproject_dict['call_rcn'] = q2['relations']['associations']['call']['rcn']
    if q2['relations']['associations']['call'].has_key('title'):
        xproject_dict['call_title'] = q2['relations']['associations']['call']['title']
    xproject_dict['call_identifier'] = q2['relations']['associations']['call']['identifier']

    # programmes data

    x_progs = q2['relations']['associations']['programme']

    for x_prog in x_progs:
        xproject_dict_prog = dict.fromkeys(xFields_project_programme_h2020)

        xproject_dict_prog['project_rcn'] = q2['rcn']
        xproject_dict_prog['programme_type'] = x_prog['@type']
        xproject_dict_prog['programme_rcn'] = x_prog['rcn']
        xproject_dict_prog['programme_code'] = x_prog['code']
        xproject_dict_prog['programme_title'] = x_prog['title']

        # some info exists only for related programmes (and not sub programmes )
        if x_prog.has_key('frameworkProgramme'):
            xproject_dict_prog['programme_frameworkProgramme'] = x_prog['frameworkProgramme']
        if x_prog.has_key('shortTitle'):
            xproject_dict_prog['programme_shortTitle'] = x_prog['shortTitle']
            # if x_prog.has_key('url'):
        #    xproject_dict_prog['programme_url'] = x_prog['url']

        xproject_lst_prog.append(xproject_dict_prog)

    # organisations data
    x_orgs = q2['relations']['associations']['organization']
    # in some cases you have one organisations and it is not a list
    if type(x_orgs) != list:
        x_orgs = [x_orgs]

    for x_org in x_orgs:
        xproject_dict_org = dict.fromkeys(xFields_project_organisation_h2020)
        xproject_dict_org['project_rcn'] = q2['rcn']
        xproject_dict_org['org_order'] = x_org['@order']
        xproject_dict_org['org_type'] = x_org['@type']
        xproject_dict_org['org_rcn'] = x_org['rcn']
        xproject_dict_org['org_id'] = x_org['id']
        xproject_dict_org['org_legalName'] = x_org['legalName']
        xproject_dict_org['org_shortName'] = x_org['shortName']

        if x_org.has_key('address'):
            if x_org['address'].has_key('street'):
                xproject_dict_org['org_adress_street'] = x_org['address']['street']
            if x_org['address'].has_key('city'):
                xproject_dict_org['org_adress_city'] = x_org['address']['city']
            if x_org['address'].has_key('postalCode'):
                xproject_dict_org['org_adress_postalCode'] = x_org['address']['postalCode']
            if x_org['address'].has_key('country'):
                xproject_dict_org['org_adress_country'] = x_org['address']['country']
            if x_org['address'].has_key('geolocation'):
                xproject_dict_org['org_adress_geolocation'] = x_org['address']['geolocation']

        # change from H2020 : department and geolocation
        #if x_org.has_key('departmentName'):
        #    xproject_dict_org['org_departmentName'] = x_org['departmentName']

            # also: only H2020 data have ecContribution at level of organisation
        # and status : terminated or not
        if x_org.has_key('@ecContribution'):
            xproject_dict_org['org_ecContribution'] = x_org['@ecContribution']
        if x_org.has_key('@terminated'):
            xproject_dict_org['org_terminated'] = x_org['@terminated']

        xproject_lst_org.append(xproject_dict_org)

    xresult_dict['data_project'] = xproject_dict
    xresult_dict['data_project_prog'] = xproject_lst_prog
    xresult_dict['data_project_org'] = xproject_lst_org

    return xresult_dict


#xFile_h2020 = '/media/mike/MyDataContainer/1000_ScientoMetricData/___Staging/1000_GRANTS/3000_EU_FP/cordis-h2020projects-xml/project-rcn-193203_en.xml'
#with open(xFile_h2020, 'r') as f:
#    xData = f.read()

