from . import extract_notion as notion
from utility_files import utility_functions as util
import pandas as pd
import json
from config import config as cfg
import logging
from datetime import datetime
from fnmatch import fnmatch

today = datetime.now()  # current date and time
year = today.strftime("%Y")
month = today.strftime("%m")
day = today.strftime("%d")
hour = today.strftime("%H")
logname = "log/Run-" + year + "-" + month + "-" + day + "-" + hour + ".log"
logging.basicConfig(filename=logname,
                    filemode='a',
                    format='%(asctime)s:%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.INFO)


# cleans a raw response/ extracts all useful fields from raw reponse as texts/lists/numbers
def transform_to_silver(json_dict):
    final_json = []

    raw_list =json_dict
    for role in raw_list:
        temp_dict = {}
        temp_dict['id'] = role['id']
        temp_dict['created_time'] = role['created_time']
        temp_dict['last_edited_time'] = role['last_edited_time']

        try:
            temp_dict['workbook_id'] = role['workbook_id']
        except:
            pass

        properties = role['properties']
        for key, value in properties.items():
            cleansed_key = key.replace(' ','_').replace("'","")
            if (properties[key]['type'] == 'relation'):
                ids = []
                for id in value['relation']:
                    ids.append(id['id'])
                if(len(ids)==0):
                    ids = None
                temp_dict[cleansed_key] = ids
            elif(properties[key]['type'] == 'rich_text'):
                texts = []
                for text in value['rich_text']:
                    if text['type'] == 'text':
                        texts.append(text['plain_text'])
                if (len(texts) == 0):
                    texts = None
                if texts != None:
                    texts = texts[0]
                temp_dict[cleansed_key] = texts

            elif (properties[key]['type'] == 'title'):
                texts = []
                for text in value['title']:
                    if text['type'] == 'text':
                        texts.append(text['plain_text'])
                if (len(texts) == 0):
                    texts = None
                if texts != None:
                    texts = texts[0]
                temp_dict[cleansed_key] = texts
            elif (properties[key]['type'] == 'select'):
                temp_dict[cleansed_key] = properties[key]['select']['name']
            elif (properties[key]['type'] == 'url'):
                temp_dict[cleansed_key] = properties[key]['url']
            elif (properties[key]['type'] == 'checkbox'):
                temp_dict[cleansed_key] = properties[key]['checkbox']
            elif (properties[key]['type'] == 'date'):
                temp_dict[cleansed_key] = properties[key]['date']['start']
            elif (properties[key]['type'] == 'number'):
                temp_dict[cleansed_key] = properties[key]['number']
            elif (properties[key]['type'] == 'people'):
                people = []
                for name in value['people']:
                    people.append(name['name'])
                if (len(people) == 0):
                    people = None
                temp_dict[cleansed_key] = people
            elif (properties[key]['type'] == 'files'):
                temp_dict[cleansed_key] = properties[key]['files']
        final_json.append(temp_dict)

    return final_json

# expired
def process_items(ids_list):
    results = ''
    for key, value in ids_list.items():
        bronze_actionitems = notion.store_database(value, key)
        silver_actionitems = transform_to_silver(bronze_actionitems)

        result = util.transform_to_ndjson(silver_actionitems)
        results = results + result
        results += '\n'

    return results

# returns a dict with keys as table name and value as data for all students
def process_tables(table_ids):
    tables = {}
    results = ''
    for key, value in table_ids.items():  # process each workbook for all tables in source data
        workbook_id = key
        print(workbook_id)
        for table_dict in value:
            table_name = table_dict['table_name']
            table_id = table_dict['table_id']
            print(table_name)
            if(table_name=="LeetCode Problems"):  # skipping Leetcode problems because of huge number of rows
                continue
            bronze_table = notion.store_database(table_id, workbook_id)  # hitting database endpoint and retrieving
            # raw json response for the table
            silver_table = transform_to_silver(bronze_table)  # extracting properties

            silver_table = util.transform_to_ndjson(silver_table)  # NDJSON is required for loading data into bigquery
            silver_table = silver_table + "\n"  # adding new line for each new workbook
            if table_name not in tables:  # if table does not exist then add a table name as key
                tables[table_name] = silver_table
            else:
                tables[table_name] = tables[table_name] + silver_table

    return tables

# renames common columns for each table
def clean_for_gold(values,final_df,table_name):
    final_df.rename(columns=values, inplace=True)

    final_df[table_name+'_created_time'] = final_df[table_name+'_created_time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    final_df[table_name+'_last_edited_time'] = final_df[table_name+'_last_edited_time'].dt.strftime('%Y-%m-%dT%H:%M:%S')


def transform_to_nested(df,table_name):
    temp_df = pd.DataFrame()
    if not df.empty:

        values = {"id": f"{table_name}ID","created_time": table_name+"_created_time"
        ,"last_edited_time":table_name+"_last_edited_time","Name":table_name+"Name",
                  "Description":table_name+"Description","Link":table_name + "Link",
                  "Why_its_interesting":table_name + "Why_its_interesting"}

        clean_for_gold(values, df,table_name)
        # series = df.groupby("workbook_id").apply(
        #     lambda x: x.to_json(orient='records'))  # creates
        # # a series of items for every workbook
        # df = pd.DataFrame(series)
        # temp_df = df.rename(columns={0: table_name})

    else:
        temp_df[table_name] = None
        temp_df['workbook_id'] = None

    return df

# joins domains with parent domains (self join)
def join_domains(df_domains):
    right_domains = df_domains[['id',"Name"]]
    right_domains = right_domains.rename(columns={"id": "rightid", "Name": "ParentDomainName"})
    df_domains = df_domains.explode("Parents")
    df_domains = df_domains.merge(right_domains, left_on='Parents',right_on="rightid")
    df_domains = df_domains.drop(columns=["rightid"])

    df_domains = df_domains.rename(columns={"Name": "DomainName"})

    return df_domains


def join_offers(df_offers,df_roles):
    roles_table_name = cfg.ROLES.replace(" ","")
    
    right_roles = df_roles.drop(columns=[roles_table_name+"_created_time",
                                         roles_table_name+"_last_edited_time","workbook_id"])
    if not df_offers.empty:
        df_offers = df_offers.explode("Role")

        try:
            df_offers = df_offers.merge(right_roles, left_on='Role', right_on="RolesID",how='left')
        except:
            pass
    else:
        return pd.DataFrame()

    return df_offers

def join_roles(df_comapanies,df_level,df_domains,df_location,df_market_compensation,
               df_resumes,df_people,df_business_units,df_functional_units,df_remote_status,df_roles):

    df_comapanies = df_comapanies[["CompaniesName","CompaniesWhy_its_interesting","CompaniesID"]]

    df_level = df_level[["LevelsName","LevelsID"]]

    df_domains = df_domains[["DomainName","ParentDomainName","DomainsID"]]

    df_location = df_location[["City","Country","Region","LocationsID"]]


    df_market_compensation = df_market_compensation.drop(columns=["MarketCompensation"+"_created_time",
                                                                  "MarketCompensation"+"_last_edited_time",
                                                                  "workbook_id"])

    df_resumes = df_resumes[["Resumes&ProfilesName","Resumes&ProfilesID"]]

    df_people = df_people[["PeopleName","PeopleID"]]

    df_business_units = df_business_units[["BusinessUnitsName","BusinessUnitsID"]]

    df_functional_units = df_functional_units[["FunctionalUnitsName","FunctionalUnitsID"]]

    df_remote_status = df_remote_status[["RemoteStatusName","RemoteStatusDescription","RemoteStatusID"]]

    df_hiring_manager = df_people.rename(columns={"PeopleName": "HiringManagerName",
                                                  "PeopleID": "HiringManagerID"})

    df_recruiters = df_people.rename(columns={"PeopleName": "RecruiterrName",
                                                  "PeopleID":"RecruiterID"})


    df_roles = df_roles.explode("Company").explode("Level").explode("Domain").explode("Location")\
        .explode("Requirements_Satisfied_by_Role")\
        .explode("Resumes_&_Profiles").explode("Contacts").explode("Business_Unit")\
        .explode("Functional_Unit").explode("Remote_Status").explode("Hiring_Manager").explode("Recruiters")

    try:
        df_roles = df_roles.merge(df_comapanies, left_on='Company', right_on="CompaniesID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_level, left_on='Level', right_on="LevelsID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_domains, left_on='Domain', right_on="DomainsID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_location, left_on='Location', right_on="LocationID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_market_compensation, left_on='Market_Compensation', right_on="MarketCompensationID",how='left')
    except:
        pass

    # try:
    #     df_roles = df_roles.merge(df, left_on='Requirements_Satisfied_by_Role', right_on="RolesID",how='left')
    # except:
    #     pass

    try:
        df_roles = df_roles.merge(df_resumes, left_on='Resumes_&_Profiles', right_on="Resumes&ProfilesID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_people, left_on='Contacts', right_on="PeopleID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_business_units, left_on='Business_Unit', right_on="BusinessUnitsID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_functional_units, left_on='Functional_Unit', right_on="FucntionalUnitsID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_remote_status, left_on='Remote_Status', right_on="RemoteStatusID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_hiring_manager, left_on='Hiring_Manager', right_on="HiringManagerID",how='left')
    except:
        pass

    try:
        df_roles = df_roles.merge(df_recruiters, left_on='Recruiters', right_on="RecruiterID",how='left')
    except:
        pass

    return df_roles


def join_interviews(df_people,df_question_retro,df_round_interviews,df_interviews):

    df_people = df_people.drop(columns=["People"+"_created_time","People"+"_last_edited_time","workbook_id"])

    df_question_retro = df_question_retro.drop(columns=["QuestionRetrospectives"+"_created_time",
                                                        "QuestionRetrospectives"+"_last_edited_time","workbook_id"])

    df_round_interviews = df_round_interviews[["InterviewRoundsName","InterviewRoundsDescription","InterviewRoundsID","workbook_id"]]

    df_interviews = df_interviews.explode("Interviewers").explode("Question_Retrospectives").explode("Round")

    try:
        df_interviews = df_people.merge(df_interviews, left_on='Interviewers', right_on="PeopleID",how='left')
    except:
        pass

    try:
        df_interviews = df_people.merge(df_question_retro, left_on='Question_Retrospectives'
                                        , right_on="QuestionRetrospectivesID",how='left')
    except:
        pass

    try:
        df_interviews = df_people.merge(df_round_interviews, left_on='Round', right_on="InterviewRoundsID",how='left')
    except:
        pass

    return df_interviews

def join_behavioral_problems(df_behavioral_pattern,df_behavioral_problem):

    df_behavioral_pattern = df_behavioral_pattern.drop(columns=["BehavioralPatterns"+"_created_time",
                                                                "BehavioralPatterns"+"_last_edited_time","workbook_id"])
    df_behavioral_problem = df_behavioral_problem.explode("Pattern")

    try:
        df_behavioral_problem = df_behavioral_problem.merge(df_behavioral_pattern, left_on='Pattern', right_on="BehavioralPatternsID",how='left')
    except:
        pass

    return df_behavioral_problem

def join_people(df_companies,df_people):
    df_companies = df_companies[["CompaniesName","CompaniesID"]]
    df_companies = df_companies.rename(columns={"Name": "AffiliatedCompanyName"})

    df_people = df_people.explode("Affiliated_Companies")

    try:
        df_people = df_people.merge(df_companies, left_on='Affiliated_Companies', right_on="CompaniesID",how='left')
    except:
        pass

    return df_people

def join_question_retro(df_behavioral_problem,df_question_retro):

    df_behavioral_problem = df_behavioral_problem[["BehavioralProblemEquivalent","BehavioralProblemsID"]]

    df_question_retro.explode("Behavioral_Problem_Equivalent")

    try:
        df_question_retro = df_question_retro.merge(df_behavioral_problem, left_on='Behavioral_Problem_Equivalent',
                                                            right_on="BehavioralProblemsID",how='left')
    except:
        pass

    return df_question_retro


def join_requirements(df_roles,df_companies,df_requirements):
    df_roles = df_roles[["Title","RolesID"]]
    df_roles = df_roles.rename(columns={"Title": "RolesSatisfyingTitle"})

    df_comapanies = df_companies[["CompaniesName","CompaniesID"]]
    df_comapanies = df_comapanies.rename(columns={"CompaniesName": "CompaniesSatisfyingName"})

    df_requirements = df_requirements.explode("Companies_Satisfying").explode("Roles_Satisfying")


    try:
        df_requirements = df_requirements.merge(df_roles, left_on='Roles_Satisfying', right_on="RolesID",how='left')
    except:
        pass

    try:
        df_requirements = df_requirements.merge(df_comapanies, left_on='Companies_Satisfying', right_on="CompaniesID",how='left')
    except:
        pass

    return df_requirements

def get_table_df(table_name,drop_list):
    table_name = cfg.LOCATIONS.replace(" ", "")
    df_locations = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.LOCATIONS}.json", lines=True)
    df_locations = df_locations.drop(columns=["Roles"])

# takes in students and all silver tables and do the necessary joins and outputs a final table with nested columns
# for each table
def transform_to_gold(students,tables):
    final_df = pd.DataFrame()

    df_student = pd.read_json(students,lines=True)

    df_student = df_student.explode("Workbooks")
    df_student = df_student.rename(columns={'Workbooks':'workbook_id','Name':'StudentName'
        ,'Universal_ID':'StudentUniversalID'})
    df_student = df_student[["StudentUniversalID","workbook_id","StudentName"]]

    logging.info("Transforming Exercises")
    # preparing Exercises
    table_name = cfg.EXERCISES.replace(" ", "")
    df_exercises = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.EXERCISES}.json", lines=True)

    df_exercises = transform_to_nested(df_exercises, table_name)  # TODO: fix function's name

    logging.info("Transforming Action Items")
    # preparing Action Items
    table_name = cfg.ACTION_ITEMS.replace(" ","")
    df_action_items = pd.read_json(f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.ACTION_ITEMS}.json", lines=True)

    df_action_items = transform_to_nested(df_action_items, table_name)

    logging.info("Transforming Locations")
    # preparing Locations
    table_name = cfg.LOCATIONS.replace(" ", "")
    df_locations = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.LOCATIONS}.json", lines=True)
    if not df_locations.empty:
        df_locations = df_locations.drop(columns=["Roles"])
        df_locations = transform_to_nested(df_locations, table_name)

    logging.info("Transforming Domains")
    # preparing Domains
    table_name = cfg.DOMAINS.replace(" ","")
    df_domains = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.DOMAINS}.json", lines=True)
    if not df_domains.empty:
        df_domains = df_domains.drop(columns=["Roles"])
        df_domains = join_domains(df_domains)
        df_domains = transform_to_nested(df_domains, table_name)

    logging.info("Transforming Levels")
    # preparing Levels
    table_name = cfg.LEVEL.replace(" ", "")
    df_levels = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.LEVEL}.json", lines=True)
    if not df_levels.empty:
        df_levels = df_levels.drop(columns=["Roles"])
        df_levels = transform_to_nested(df_levels, table_name)

    logging.info("Transforming Resumes and Profiles")
    # preparing Resumes and Profiles
    table_name = cfg.RESUME_PROFILES.replace(" ", "")
    df_resumes = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.RESUME_PROFILES}.json", lines=True)
    if not df_resumes.empty:
        df_resumes = df_resumes.drop(columns=["Files","Targeted_Role"])  # removing Files cause link is already available
        df_resumes = transform_to_nested(df_resumes, table_name)

    logging.info("Transforming Remote Status")
    # preparing Remote Status
    table_name = cfg.REMOTE_STATUS.replace(" ", "")
    df_remote_status = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.REMOTE_STATUS}.json", lines=True)
    if not df_remote_status.empty:
        df_remote_status = df_remote_status.drop(columns=["Roles"])
        df_remote_status = transform_to_nested(df_remote_status, table_name)

    logging.info("Transforming Companies")
    # preparing Companies
    table_name = cfg.COMPANIES.replace(" ", "")
    df_companies = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.COMPANIES}.json", lines=True)
    if not df_companies.empty:
        df_companies = df_companies.drop(columns=["Roles", "Contacts","Expected_Requirements_Satisfied"])
        df_companies = transform_to_nested(df_companies, table_name)

    # preparing People
    table_name = cfg.PEOPLE.replace(" ","")
    df_people = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.PEOPLE}.json", lines=True)
    if not df_people.empty:
        df_people = df_people.drop(columns=["Interviews_Given"])
        df_people = join_people(df_companies,df_people)
        df_people= transform_to_nested(df_people,table_name)

    # preparing Business Units
    table_name = cfg.BUSINESS_UNITS.replace(" ","")
    df_business_units = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.BUSINESS_UNITS}.json", lines=True)
    if not df_business_units.empty:
        df_business_units = df_business_units.drop(columns=["Roles"])
        df_business_units = transform_to_nested(df_business_units, table_name)

    # preparing Functional Units
    table_name = cfg.FUNCTION_UNITS.replace(" ","")
    df_functional_units = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.FUNCTION_UNITS}.json", lines=True)
    if not df_functional_units.empty:
        df_functional_units = df_functional_units.drop(columns=["Roles"])
        df_functional_units = transform_to_nested(df_functional_units, table_name)

    # preparing Interview Rounds
    table_name = cfg.INTERVIEW_ROUNDS.replace(" ", "")

    df_interview_rounds = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.INTERVIEW_ROUNDS}.json", lines=True)
    if not df_interview_rounds.empty:
        df_interview_rounds = df_interview_rounds.drop(columns=["Interviews"])
        df_interview_rounds = transform_to_nested(df_interview_rounds, table_name)

    # preparing Question Retrospectives
    table_name = cfg.QUESTION_RETROSPECTIVES.replace(" ", "")

    df_question_retro = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.QUESTION_RETROSPECTIVES}.json",
        lines=True)
    if not df_question_retro.empty:
        df_question_retro = df_question_retro.drop(columns=["Interview"])
        # TODO: do sth about joins for problems
        df_question_retro = df_question_retro.explode("Behavioral_Problem_Equivalent").explode("LeetCode_Equivalent")
        df_question_retro = transform_to_nested(df_question_retro, table_name)

    # preparing Behavioral Patterns
    table_name = cfg.BEHAVIORAL_PATTERNS.replace(" ","")
    df_behavioral_patterns = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.BEHAVIORAL_PATTERNS}.json", lines=True)
    if not df_behavioral_patterns.empty:
        df_behavioral_patterns = df_behavioral_patterns.drop(columns=['Related_to_Behavioral_Problems_(Pattern)'])
        df_behavioral_patterns = transform_to_nested(df_behavioral_patterns, table_name)

    # preparing Behavioral Problems
    table_name = cfg.BEHAVIORAL_PROBLEMS.replace(" ", "")
    df_behavioral_problems = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.BEHAVIORAL_PROBLEMS}.json", lines=True)
    if not df_behavioral_problems.empty:
        df_behavioral_problems = df_behavioral_problems.drop(columns=["Question_Retrospectives"])
        df_behavioral_problems = join_behavioral_problems(df_behavioral_patterns,df_behavioral_problems)
        df_behavioral_problems = transform_to_nested(df_behavioral_problems, table_name)

    # preparing Market Compensation
    table_name = cfg.MARKET_COMPENSATION.replace(" ","")

    df_market_compensation = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.MARKET_COMPENSATION}.json", lines=True)
    if not df_market_compensation.empty:
        df_market_compensation = df_market_compensation.drop(columns=['Role'])
        df_market_compensation = transform_to_nested(df_market_compensation, table_name)

    # preparing Roles
    table_name = cfg.ROLES.replace(" ", "")
    df_roles = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.ROLES}.json", lines=True)
    if not df_roles.empty:
        df_roles = join_roles(df_companies,df_levels,df_domains,
                              df_locations,df_market_compensation,df_resumes,
                              df_people,df_business_units,df_functional_units,df_remote_status,df_roles)
        df_roles = transform_to_nested(df_roles, table_name)

    # preparing Requirements
    table_name = cfg.REQUIREMENTS.replace(" ", "")
    df_requirements = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.REQUIREMENTS}.json", lines=True)

    if not df_requirements.empty:
        df_requirements = join_requirements(df_roles,df_companies,df_requirements)
        df_requirements = transform_to_nested(df_requirements, table_name)

    # preparing Offers
    table_name = cfg.OFFERS.replace(" ", "")
    df_offers = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.OFFERS}.json", lines=True)
    if not df_offers.empty:
        df_offers = join_offers(df_offers, df_roles)
        df_offers = transform_to_nested(df_offers, table_name)

    # preparing Interviews
    table_name = cfg.INTERVIEWS.replace(" ", "")
    df_interviews = pd.read_json(
        f"/Users/abdullahkhan/PycharmProjects/InterviewKickstart/{tables}/{cfg.INTERVIEWS}.json", lines=True)
    if not df_interviews.empty:
        df_interviews = df_interviews.drop(columns=["Role"])
        df_interviews = join_interviews(df_people, df_question_retro, df_interview_rounds, df_interviews)
        df_interviews = transform_to_nested(df_interviews, table_name)

    final_joined = pd.concat([df_action_items,df_exercises,df_domains,df_roles,df_people,df_behavioral_patterns,
                              df_behavioral_problems,df_business_units,df_companies,df_functional_units,
                              df_interview_rounds,df_interviews,df_levels,df_resumes,df_requirements,df_remote_status,
                              df_question_retro,df_locations,df_offers])


    if not final_joined.empty:
        final_df = final_joined.merge(df_student, on='workbook_id', how='left')

        final_df["Assigned_By"].to_list()
        final_df.columns = final_df.columns.str.replace('[#,@,&]', '')

    # print(final_df)
    # roles = transform(roles)
    # offers = transform_offers_gold(offers,roles)

    return final_df


# expired
def transform_actionItems_to_nested_gold(students,action_items):
    df_student = pd.read_json(students,lines=True)
    df_action_items = pd.read_json(action_items,lines=True)

    df_student = df_student.explode("Workbooks")
    df_student = df_student.rename(columns={'Workbooks':'workbook_id','Name':'StudentName','Universal_ID':'StudentUniversalID'})
    df_student = df_student[["StudentUniversalID","workbook_id","StudentName"]]

    final_df = df_student.merge(df_action_items, on='workbook_id', how='left')
    final_df = final_df.dropna(subset=["Name"])

    final_df['created_time'] = final_df['created_time'].dt.strftime('%Y-%m-%dT%H:%M:%S')
    final_df['last_edited_time'] = final_df['last_edited_time'].dt.strftime('%Y-%m-%dT%H:%M:%S')

    result = final_df.to_json(orient='records')
    parsed = json.loads(result)
    res = util.transform_to_ndjson(parsed)

    # final_df.to_csv("final_df.csv")
    return res
