# Script to read the notion database using API
import requests, json
import time

f = open("config/config.json")
config = json.load(f)

headers = config["headers_dev"]
last_edited_time_config = config["last_edited_time"]

#Hit database endpoint and yields pages for response
def get_databases(databaseId, headers, last_edited_time):
    readUrl = f"https://api.notion.com/v1/databases/{databaseId}/query"

    body = None

    if(last_edited_time!=None):
        body ={"filter": {
                "property": "Last Edited Time",
                "last_edited_time": {
                "after": last_edited_time
                    }
                }
        }

    res = requests.request("POST", readUrl, headers=headers,json=body)
    data = res.json()
    # print(data)
    if (res.status_code == 200):
        yield data
    else:
        return None

    while (data['has_more']==True):
        next_cursor = data['next_cursor'] # yielding pages for next 100 items
        readUrl = f"https://api.notion.com/v1/databases/{databaseId}/query?start_cursor={next_cursor}"
        res = requests.request("POST", readUrl, headers=headers,json=body)
        data = res.json()
        if (res.status_code == 200):
            yield data
        else:
            return None

# hitting blocks endpoint
def get_blocks(block_id, headers):
    time.sleep(3)
    readUrl = f"https://api.notion.com/v1/blocks/{block_id}/children"

    # print(readUrl)
    res = requests.request("GET", readUrl, headers=headers)
    data = res.json()
    if (res.status_code == 200):
        yield data
    else:
        return None

    while (data['has_more']==True):
        next_cursor = data['next_cursor']
        readUrl = f"https://api.notion.com/v1/blocks/{block_id}/children?start_cursor={next_cursor}"
        res = requests.request("GET", readUrl, headers=headers)
        data = res.json()
        if (res.status_code == 200):
            yield data
        else:
            return None

# returns results of a database
def get_parent_database(database_id,last_edited_time = last_edited_time_config):
    for page in get_databases(database_id,headers,last_edited_time):
        final_list = []
        final_list = final_list + page['results']

    return final_list

# expired
def get_exercise_ids(workbooks):
    ids = {}
    # ids = []
    workbook_count = len(workbooks)

    if(workbook_count > 0):
        for i in range(workbook_count):
            workbook_id = workbooks[i]['id']
            for page in get_blocks(workbook_id, headers):

                results = page['results']

                for result in results:
                    if((result['type'] == 'child_database')):
                        if((result['child_database']['title'] == 'Exercises')):
                            exercise_id = result['id']
                            ids[workbook_id] = exercise_id
                            # ids.append(temp)

    else:
        pass

    return ids

# expired
def get_action_items_ids(workbooks):
    ids = {}
    # ids = []
    workbook_count = len(workbooks)

    if(workbook_count > 0):
        for i in range(workbook_count):
            workbook_id = workbooks[i]['id']
            for page in get_blocks(workbook_id, headers):

                results = page['results']

                for result in results:
                    if((result['type'] == 'child_database')):
                        if((result['child_database']['title'] == 'Action Items')):
                            exercise_id = result['id']
                            ids[workbook_id] = exercise_id
                            # ids.append(temp)

    else:
        pass

    return ids


# Parse each workbook and return IDs for Internal Use and Action Items blocks
def parse_workbook(workbooks):
    action_items_ids = {}
    internal_use_ids = {}
    # ids = []
    workbook_count = len(workbooks)

    if (workbook_count > 0):
        for i in range(workbook_count):

            try:
                if ("Unassigned" in workbooks[i]['properties']['Title']['title'][0]['plain_text']):  # ignore if workbook
                    # is Unassigned
                    continue
            except:
                pass  #  pass if title is empty

            workbook_id = workbooks[i]['id']
            for page in get_blocks(workbook_id, headers):

                results = page['results']

                for result in results:
                    if ((result['type'] == 'child_database')):
                        if ((result['child_database']['title'] == 'Action Items')):
                            action_id = result['id']
                            action_items_ids[workbook_id] = action_id
                            # ids.append(temp)

                    elif ((result['type'] == 'column_list') & (result['has_children'] == True)):
                        internal_use_id = result['id']
                        internal_use_ids[workbook_id] = internal_use_id
    else:
        pass

    return internal_use_ids

# returns source data IDs for all internal use blocks
def extract_source_data_ids(internal_use_ids):
    source_data_ids = {}

    for key,value in internal_use_ids.items():
        workbook_id = key
        internal_use_id = value
        for page in get_blocks(internal_use_id, headers):
            results = page['results']
            for result in results:
                block_id = result['id']
                for nested_page in get_blocks(block_id, headers):
                    nesterd_results = nested_page['results']
                    for nested_result in nesterd_results:
                        if((nested_result['type']=='child_page')
                                & (nested_result['child_page']['title']=='Source Data')):

                            source_data_ids[workbook_id] = nested_result['id']
                            break

    return source_data_ids

# takes a key value pair of workbook id and source data and returns a dict with workbook ids as keys and a list of all
# table names and table ids
def extract_table_ids(source_data_ids):
    table_ids = {}
    for key, value in source_data_ids.items():
        workbook_id = key
        source_data_id = value
        temp_list = []
        temp_dict = {}

        for page in get_blocks(source_data_id, headers):
            results = page['results']
            for result in results:
                if (result['type'] == 'child_database'): # looks for all the blocks which are child databases and
                    # return ids and names
                    table_name = result['child_database']['title']
                    table_id = result['id']

                    temp_dict["table_name"] = table_name
                    temp_dict["table_id"] = table_id

                    dictionary_copy = temp_dict.copy()
                    temp_list.append(dictionary_copy)

        table_ids[workbook_id] = temp_list

    return table_ids

# expired
def get_exersice_page(exersice_id,exersice_name):
    offer_evaluation_page_id = None

    for page in get_databases(exersice_id,headers):
        results = page['results']
        for result in results:
            if ((result["properties"]["Name"]["title"][0]["plain_text"].strip() == exersice_name)):
                    offer_evaluation_page_id = result['id']
                    break
            else:
                pass

        return offer_evaluation_page_id

# expired
def get_table_id(exersice_id, table_name):
    table_id = None

    for page in get_blocks(exersice_id, headers):
        results = page['results']

        for result in results:
            if ((result['type'] == 'child_database')):
                if ((result['child_database']['title'] == table_name)):
                    table_id = result['id']
                    break

        else:
            pass

        return table_id

# returns database ids with related workbook ids
def store_database(database_id,workbook_id, last_edited_time = last_edited_time_config):
    results = []
    for page in get_databases(database_id,headers,last_edited_time):
        results = results + page['results']

    for result in results:
        result['workbook_id'] = workbook_id

    # jsonString = json.dumps(results)
    # with open(f'{filename}.json', 'w', encoding='utf8') as f:
    #     json.dump(jsonString, f, ensure_ascii=False)

    return results

# print(roles_id)