import sys
import os
from config import config as cfg
from src import extract_notion as notion
from src import json_parsers as parsers
from src import bigqueryLoad as load
from utility_files import utility_functions as util
import json
import logging
import traceback

sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))

from datetime import datetime


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

def main():

    f = open("config/config.json")
    config = json.load(f)

    print("Last Run: ",config["last_edited_time"])

    # stores last edited time as a checkpoint for the next run
    config["last_edited_time"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


    logging.info("Pipeline Started ------------")

    #set environment credentails
    STUDENTS_ID = config["STUDENT_DATABASE_UPDOGDEV"]
    WORKBOOKS_ID = config["WORKBOOK_DATABASE_UPDOGDEV"]

    table_id_path = "TableIDsDev"

    try:
        # Retrieving and processing students table
        logging.info("Retrieving Students")
        students = notion.get_parent_database(STUDENTS_ID,None)
        if(students != None):
            logging.info("Processing Students")
            silver_students = parsers.transform_to_silver(students)
            silver_students = util.transform_to_ndjson(silver_students)
        else:
            logging.info("No new students found!")
            return 0

        # Retrieving all workbooks from workbook database
        logging.info("Retrieving Workbooks")
        workbooks = notion.get_parent_database(WORKBOOKS_ID,None)

        if(workbooks != None):
            logging.info("Retrieving Source Data IDs")
            internal_use_ids = notion.parse_workbook(workbooks)  # Retrieving "Internal Use" block IDs for each workbook
            source_data_ids = notion.extract_source_data_ids(internal_use_ids)  # for each Internal Use, find Source Data block

            logging.info("Retrieving Table IDs")
            table_ids = notion.extract_table_ids(source_data_ids)  # Retrieving Table IDs for each workbook
            table_ids = json.dumps(table_ids)
            util.store_json(table_ids,table_id_path)

            logging.info("Table IDs Stored!")
        else:
            logging.info("Workbooks not updated!")

        f = open(table_id_path + ".json")  # Table IDs are stored as a separate file in
        table_ids = json.load(f)

        logging.info("Processing Table IDs")
        # retrieving and transforming tables for all students and persisting silver tables
        silver_tables = parsers.process_tables(table_ids)
        util.store_tables(silver_tables)
        logging.info("Silver tables stored!")

        logging.info("Transforming tables to Gold")
        final_df = parsers.transform_to_gold(silver_students,"silver_tables")

        final_ndjson = final_df.to_json(lines=True,orient='records')

        # final_df.to_csv("gold_table.csv")

        with open("gold_table.json", 'w',
                  encoding='utf8') as f:
            f.write(final_ndjson)
            f.close()

        # with open("config/config.json", "w") as write_file:
        #     json.dump(config, write_file, indent=4)

        load.load_data()
        load.dedup_final_table()

    except Exception as e:
        logging.error(traceback.format_exc())
        print(traceback.format_exc())

if __name__ == "__main__":
    main()
