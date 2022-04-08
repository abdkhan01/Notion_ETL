import json

def transform_to_ndjson(silver_json):
    result = [json.dumps(record) for record in silver_json]
    result = '\n'.join(result)
    return result

def store_tables(tables):
    for key,value in tables.items():
        with open("/Users/abdullahkhan/PycharmProjects/InterviewKickstart/silver_tables/" + key + ".json", 'w', encoding='utf8') as f:
                f.write(value)
                f.close()

def store_json(content,path):
    with open(path + ".json", 'w',
              encoding='utf8') as f:
        f.write(content)
        f.close()