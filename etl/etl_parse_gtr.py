'''
parsing files from gateway to research
'''

import json


def gtr_parse_projects(xFile):
    '''
    #parse project data from Gateway to research
    RETURN:
    return a dictionary with project data (list) and a list of "relations"
    #N.B: metadata on relations such as PI, Funding Organisation or Performing Organisation
    # should be fetched separately
    #for example given their IDs

    '''

    xDictRes = {}

    xlst_links_res = []  # results links
    xlst_projectData = []

    with open(xFile, 'r') as ff:
        xData = ff.read()
    xDataJson = json.loads(xData)
    xlst_projects = xDataJson['project']

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

    for x_project in xlst_projects:

        # links
        xFields_links = ['id', 'href', 'rel']

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

        xlst_projectData.append(x_data_project)

        # process links
        xlst_links = x_project['links']['link']

        for xDict in xlst_links:
            xDictLink = dict.fromkeys(xFields_links)
            xDictLink['id'] = x_project['id']
            xDictLink['href'] = xDict['href']
            xDictLink['rel'] = xDict['rel']
            xlst_links_res.append(xDictLink)

    xDictRes['grant_data'] = xlst_projectData
    xDictRes['grant_links'] = xlst_links_res

    return xDictRes


def gtr_parse_org(xFile):
    '''
    parse Files for Organisations
    return : a list of dictionary with
    org_id and org_name and main adress county and town
    '''

    xres_list_orgs = []

    with open(xFile, 'r') as ff:
        xData = ff.read()
    xDataJson = json.loads(xData)

    xlst_orgs = xDataJson['organisation']

    for x_org in xlst_orgs:
        xDictRes = dict.fromkeys(['org_id', 'org_name',
                                  'address_main_county', 'address_main_region',
                                  'address_main_postcode', 'address_main_line1'])

        xDictRes['org_id'] = x_org['id']
        xDictRes['org_name'] = x_org['name']

        if x_org.has_key('addresses') and (x_org['addresses']).has_key('address'):
            xlst_addresses = x_org['addresses']['address']
            for x_address in xlst_addresses:
                if x_address['type'] == 'MAIN_ADDRESS':
                    if x_address.has_key('county'):
                        xDictRes['address_main_county'] = x_address['county']
                    if x_address.has_key('region'):
                        xDictRes['address_main_region'] = x_address['region']
                    if x_address.has_key('postCode'):
                        xDictRes['address_main_postcode'] = x_address['postCode']
                    if x_address.has_key('line1'):
                        xDictRes['address_main_line1'] = x_address['line1']

        xres_list_orgs.append(xDictRes)

    return xres_list_orgs


def gtr_parse_person(xFile):
    '''
    parse Files for Person
    return : a list of dictionary with
    id and names
    '''
    xres_list_persons = []

    with open(xFile, 'r') as ff:
        xData = ff.read()
    xDataJson = json.loads(xData)
    xlst_persons = xDataJson['person']

    for x_person in xlst_persons:
        xDictRes = dict.fromkeys(['person_id',
                                  'person_name_last',
                                  'person_name_first',
                                  'person_name_others'])

        xDictRes['person_id'] = x_person['id']
        xDictRes['person_name_last'] = x_person['surname']
        xDictRes['person_name_first'] = x_person['firstName']
        xDictRes['person_name_others'] = x_person['otherNames']

        xres_list_persons.append(xDictRes)

    return xres_list_persons