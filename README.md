# Project

A python ETL script to incrementally extract data from workbooks and students from notion’s backend using official Notion API and load data into big query

# Design

Script uses the Notion API to to access the backend for the workbooks and students databases. For students, every student is retrieved with name and UUIDs and workbook id. For workbooks, “Source Data” block is extract from the tables for every student. 

Transformation is being done using custom parsers. These parsers parse the API responses and extract the specified attributes. Two logical transformation zones are being followed. 

## Silver Zone

Raw responses for each table are parsed and texts, numbers, dates, relations, and URLs are extracted. IDs,created_time and last_edited_time are renamed for each table. 

## Gold Zone

All tables are denormalized and created into a flat table

Flat table is dumped into big query

# Usage

## Project Structure

### config/config.json

a JSON file which includes all the environment configurations and constants for table names as key value pairs.

### src/extract_notion.py

includes functions extraction functions which hit Notion API endpoints for databases and blocks and returns results for all the specified databases.

### src/json_parsers.py

includes functions to process bronze data into silver tables. It extracts useful fields from the raw response coming from the extract_notion.py

### src/bigqueryLoad.py

script to load the gold json file to bigquery and deleted duplicates.

### main.py

Implements the order of ETL and stores the json file for gold table locally.

# How To Run

1. Environment can be configured in config.json as:

``` {
  "TOKEN_DEV" = "secret_<your_integration_secret>"
  "headers_dev" = {
      "Authorization": "Bearer " + TOKEN_DEV,    
      "Content-Type": "application/json",    
      "Notion-Version": "2021-05-13"
      }
  "WORKBOOK_DATABASE_DEV" = "<workbook_database_id>"
  "STUDENT_DATABASE_DEV" = "<student_database_id>""
} 
```

2. In main.py replace STUDENTS_ID and WORKBOOK_ID with STUDENT_DATABASE_DEV and WORKBOOK_DATABASE_DEV from config.py
3. In extract_notion.py replace headers with headers_dev with config.py
4. run main.py

# Caveats
* Script is not yet flexible for an inconsistent workbook front end.
* Table names are hardcoded in the config.py
* Only following entities will be extracted using the script
 * relation
 * text
 * title
 * URL
 * checkboxes
 * dates
 * people
* Free form text fields are not being extracted
 
