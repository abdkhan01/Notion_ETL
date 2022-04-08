from google.cloud import bigquery
import json
from os.path import dirname, abspath
d = dirname(dirname(abspath(__file__)))
import os

f = open(d + "/config/config.json")
config = json.load(f)

# Construct a BigQuery client object.
client = bigquery.Client()
table_id = "testprojectnotioningestion.notion_dataset_test.flat_json_updogdev"

def load_data():


    job_config = bigquery.LoadJobConfig(
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )


    with open(d + "/gold_table.json", 'rb') as source_file:
        check = os.stat(d + "/gold_table.json").st_size == 0

        if(check != True):
            job = client.load_table_from_file(source_file, table_id, job_config=job_config)
            job.result()  # Waits for the job to complete.
        else:
            print("No New Data available")
            exit()

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

#dedup final table
def dedup_final_table():

    table_names = config["table_names"]

    for key,value in table_names.items():
        value = value.replace(" ","")
        value = value.replace("&", "")

        print("Deduping " + value)

        if (value=="MarketCompensation"):
            continue

        dedup_query = f"""delete FROM `{table_id}`
        WHERE STRUCT({value}ID, {value}_last_edited_time)
        NOT IN (
                SELECT AS STRUCT {value}ID,MAX({value}_last_edited_time) as {value}_last_edited_time
                 FROM `notion_dataset_test.flat_json_dev`
                 where {value}ID is not NULL
                 GROUP BY {value}ID)"""

        try:
            query_job = client.query(dedup_query)  # Make an API request.
        except:
            pass



