
# parsing nsf grant file




import zipfile
import xmltodict
import pandas as pd


def parse_nsf_grant_to_csv(xFile):
    '''
    input : ziped file of the year

    principle : read-once
    we use pandas
    i.e we create 4 separate  dataframes
    '''

    xlstRes_grant_info = []
    xlstRes_grant_person = []
    xlstRes_grant_institution = []
    xlstRes_grant_programme = []

    with zipfile.ZipFile(xFile, "r") as f:
        for name in f.namelist():
            # i = i +1
            data = f.read(name)
            q1 = xmltodict.parse(data)
            q2 = q1['rootTag']['Award']

            # grant information
            xDict_grant = {}
            xDict_grant['AwardID'] = q2['AwardID']
            xDict_grant['AwardTitle'] = q2['AwardTitle']
            xDict_grant['AwardAmount'] = q2['AwardAmount']
            xDict_grant['AbstractNarration'] = q2['AbstractNarration']  ## remove html marks

            xDict_grant['AwardEffectiveDate'] = q2['AwardEffectiveDate']
            xDict_grant['AwardExpirationDate'] = q2['AwardExpirationDate']
            xDict_grant['MinAmdLetterDate'] = q2['MinAmdLetterDate']
            xDict_grant['MaxAmdLetterDate'] = q2['MaxAmdLetterDate']
            xDict_grant['AwardInstrument'] = q2['AwardInstrument']['Value']
            xDict_grant['Organization_Code'] = q2['Organization']['Code']
            xDict_grant['Organization_Directorate'] = q2['Organization']['Directorate']['LongName']
            xDict_grant['Organization_Division'] = q2['Organization']['Division']['LongName']
            xDict_grant['ProgramOfficer'] = q2['ProgramOfficer']['SignBlockName']
            xDict_grant['ARRAAmount'] = q2['ARRAAmount']

            xlstRes_grant_info.append(xDict_grant)

            # get person
            if q2.has_key('Investigator'):

                if type(q2['Investigator']) != list:
                    x_data_investigator = [dict(q2['Investigator'])]
                else:
                    x_data_investigator = q2['Investigator']

                for x_pers in x_data_investigator:
                    xDict_person = {}
                    xDict_person['AwardID'] = q2['AwardID']
                    xDict_person['FirstName'] = x_pers['FirstName']
                    xDict_person['LastName'] = x_pers['LastName']
                    xDict_person['EmailAddress'] = x_pers['EmailAddress']
                    xDict_person['StartDate'] = x_pers['StartDate']
                    xDict_person['EndDate'] = x_pers['EndDate']
                    xDict_person['RoleCode'] = x_pers['RoleCode']

                    xlstRes_grant_person.append(xDict_person)

            # institution
            if type(q2['Institution']) != list:
                x_data_institution = [dict(q2['Institution'])]
            else:
                x_data_institution = q2['Institution']

            for x_inst in x_data_institution:
                xDict_institution = {}

                xDict_institution['AwardID'] = q2['AwardID']

                xDict_institution['Name'] = x_inst['Name']
                xDict_institution['CityName'] = x_inst['CityName']
                xDict_institution['ZipCode'] = x_inst['ZipCode']
                xDict_institution['PhoneNumber'] = x_inst['PhoneNumber']
                xDict_institution['StreetAddress'] = x_inst['StreetAddress']
                xDict_institution['CountryName'] = x_inst['CountryName']
                xDict_institution['StateName'] = x_inst['StateName']
                xDict_institution['StateCode'] = x_inst['StateCode']

                xlstRes_grant_institution.append(xDict_institution)

                '''
                #ProgramElement 
                if q2.has_key('ProgramElement'):
                    if type(q2['ProgramElement']) != list:
                        x_data_programElement = [dict(q2['ProgramElement'])]
                    else:
                        x_data_programElement = q2['ProgramElement']

                for x_prog_el in x_data_programElement:
                    xDict_programElement = {}
                    xDict_programElement['AwardID'] = q2['AwardID']     
                    xDict_programElement['ProgInfo'] = 'ProgramElement'  
                    xDict_programElement['Code'] =x_prog_el['Code']
                    xDict_programElement['Text'] =x_prog_el['Text']
                    xlstRes_grant_programme.append(xDict_programElement)

                #ProgramReference     
                if q2.has_key('ProgramReference'):
                    if type(q2['ProgramReference']) != list:
                        x_data_programReference = [dict(q2['ProgramReference'])]
                    else:
                        x_data_programReference = q2['ProgramElement']

                for x_prog_Ref in x_data_programReference:
                    xDict_programReference = {}
                    xDict_programReference['AwardID'] = q2['AwardID']        
                    xDict_programReference['ProgInfo'] = 'ProgramReference'  
                    xDict_programReference['Code'] =x_prog_Ref['Code']
                    xDict_programReference['Text'] =x_prog_Ref['Text']
                    xlstRes_grant_programme.append(xDict_programReference)  
               '''

    return {'grant_info': pd.DataFrame(xlstRes_grant_info),
            'grant_person': pd.DataFrame(xlstRes_grant_person),
            'grant_institution': pd.DataFrame(xlstRes_grant_institution),
            'grant_programme': pd.DataFrame(xlstRes_grant_programme)}