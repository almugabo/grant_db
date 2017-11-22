
# coding: utf-8

# # Parse files from NIH
# ### for now only 2006 to 2016


# data already in csv and relatively large for pandas
# we will use direct read in the postgres

# for every year, we will create two tables
# (1) projects and (2) abstracts

create table nih_projects
(APPLICATION_ID              int64
ACTIVITY                   object
ADMINISTERING_IC           object
APPLICATION_TYPE            int64
ARRA_FUNDED               float64
AWARD_NOTICE_DATE          object
BUDGET_START               object
BUDGET_END                 object
CFDA_CODE                 float64
CORE_PROJECT_NUM           object
ED_INST_TYPE               object
FOA_NUMBER                 object
FULL_PROJECT_NUM           object
FUNDING_ICs                object
FY                          int64
IC_NAME                    object
NIH_SPENDING_CATS         float64
ORG_CITY                   object
ORG_COUNTRY                object
ORG_DEPT                   object
ORG_DISTRICT              float64
ORG_DUNS                  float64
ORG_FIPS                   object
ORG_NAME                   object
ORG_STATE                  object
ORG_ZIPCODE               float64
PHR                       float64
PI_IDS                     object
PI_NAMEs                   object
PROGRAM_OFFICER_NAME       object
PROJECT_START              object
PROJECT_END                object
PROJECT_TERMS              object
PROJECT_TITLE              object
SERIAL_NUMBER               int64
STUDY_SECTION              object
STUDY_SECTION_NAME         object
SUBPROJECT_ID             float64
SUFFIX                     object
SUPPORT_YEAR                int64
TOTAL_COST                float64
TOTAL_COST_SUB_PROJECT    float64

nih_abstracts

APPLICATION_ID     int64
ABSTRACT_TEXT     object
dtype: object

