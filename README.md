# Project

A python ETL script to incrementally extract data from workbooks and students from notion’s backend using official Notion API and load data into big query

# Design

Script uses the Notion API to to access the backend for databases. Every entity is retrieved with name and UUIDs. “Source Data” block is extract from the tables for every entity. 

Transformation is being done using custom parsers. These parsers parse the API responses and extract the specified attributes. Two logical transformation zones are being followed.

## Silver Zone

Raw responses for each table are parsed and texts, numbers, dates, relations, and URLs are extracted. IDs,created_time and last_edited_time are renamed for each table. Tables are joined using the business logic.

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

## Monitoring
a log folder is maintained for every run with complete details of the run and error traces.

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
 
