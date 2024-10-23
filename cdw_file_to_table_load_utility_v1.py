"""
This module provides utilities for performing ETL tasks: LOADING FILE'S DATA TO A TABLE
Created Date: 2023-07-01
Author: Paresh Devlekar
Last Updated Date: 2023-07-01
"""

import oracledb
import boto3
import pandas as pd
from datetime import datetime
import csv
from variables_dev import cdw_variables as cdw_vars
import sys
import math
import re

if sys.version_info[0] < 3:
    from StringIO import StringIO   # Python 2.x
else:
    from io import StringIO   # Python 3.x
    from io import BytesIO    # Python 3.x

def read_from_s3_and_write_to_oracle(
        source_filepath, 
        staging_tablename, 
        stagingTableOptionalColumns = [], 
        dateColumnConversion = []
    ):    
    
    def connect_to_oracledb():
        try:
            # Initializes the Oracle client library (assuming it's installed)
            oracledb.init_oracle_client()
            # Establishes a connection to the Oracle database using credentials and connection details stored in cdw_vars (module containing configuration variables)
            connection = oracledb.connect(
                user=cdw_vars.CDW_USER,
                password=cdw_vars.CDW_PASS,
                host=cdw_vars.HOST,
                port=cdw_vars.PORT,
                service_name=cdw_vars.SERVICE_NAME) 
            print(connection)   # Prints the connection information for debugging purpose
            return connection   # If successful, the connection object is returned
        except Exception as e:
            print(f'Error connecting to the database: {e}')
            return None
    
    def delete_existing_data_before_load(staging_tablename):
        try:
            connection = connect_to_oracledb()
            cursor = connection.cursor()
            truncate_query = f'TRUNCATE TABLE {staging_tablename}'
            # Executes the constructed truncate query using the cursor object.
            cursor.execute(truncate_query)
            print(f'Truncated table {staging_tablename}')
        except Exception as e:
            print(f'Error truncating table - {e}')
        # finally block guarantees that the code within it executes always, even if exceptions occur during the function's main execution or within the try or except blocks.
        # In database operations, it's crucial to close cursors and connections when you're done with them to avoid resource leaks and potential issues with subsequent database interactions.
        finally:
            if cursor:
                cursor.close()  # Close the database cursor object, releasing resources associated with it.
            if connection:
                connection.close()  # Close the database connection, freeing up resources on the database server.
    
    def validate_by_count_query(staging_tablename):
        try:
            connection = connect_to_oracledb()
            cursor = connection.cursor()
            count_validation_query = f'SELECT COUNT(*) AS ct_recs FROM {staging_tablename}'
            # Executes the constructed count query using the cursor object.
            cursor.execute(count_validation_query)
            # Retrives the first row (since it's a count query, there's only one row) from the result set. The first element of that row contains the count value, which is assigned to the variable records_count.
            records_count = cursor.fetchone()[0]
            print(f'Number of rows in {staging_tablename} - {records_count}', end='\n\n')
        except Exception as e:
            print(f'Failed to retrieve record count - {e}')
        # In database operations, it's crucial to close cursors and connections when you're done with them to avoid resource leaks and potential issues with subsequent database interactions. 
        finally:
            if cursor:
                cursor.close()  # Close the database cursor object, releasing resources associated with it.
            if connection:
                connection.close()  # Close the database connection, freeing up resources on the database server.

    def retrieve_staging_table_column_names(staging_tablename, stagingTableOptionalColumns):
        try:
            connection = connect_to_oracledb()
            cursor = connection.cursor()      

            # The query selects the `column_name` from `sys.all_tab_columns` view , which contains metadata about all columns in all tables.
            queryToFetchTableColumnNames = f"SELECT column_name FROM sys.all_tab_columns WHERE owner = UPPER('{staging_tablename.split('.')[0]}') AND table_name = UPPER('{staging_tablename.split('.')[1]}') ORDER BY column_id"
            print(queryToFetchTableColumnNames)     # Ex - SELECT column_name FROM sys.all_tab_columns WHERE owner = UPPER('CDW_STG') AND table_name = UPPER('TBL_USR_CORR_AC_ORGCODE') ORDER BY column_id
            
            # Then, the query `queryToFetchTableColumnNames` is executed using a database cursor. This sends the SQL command to the database for processing.
            cursor.execute(queryToFetchTableColumnNames)
            
            # Retrieves all the rows returned by executed query, in this case each row corresponds to a column name in the specified table.
            records = cursor.fetchall()
            
            # Creates a pandas dataframe `stagingTableColumnsDF` from the fetched records.
            stagingTableColumnsDF = pd.DataFrame.from_records(records, columns = [x[0] for x in cursor.description])
            
            # `stagingTableColumnsDF.values` --> Gets the underlying data of the dataframe as a NumPy array. 
            # .squeeze() --> is called to reduce any dimensions of size one (Note: This is useful if there's only one column).
            # `.tolist()` converts the NumPy array to a standard Python list.
            stagingTableColumns = stagingTableColumnsDF.values.squeeze().tolist()

            # We encountered an error `ORA-00947: not enough values` because the Oracle table contained 42 columns, while the source file being ingested only had 40 columns.
            # For example - INSERT INTO CDW_STG.TBL_PERSY_PERSYEXTENDED(ACTIVITY_CENTER_CODE, FUNCTIONAL_PROCESS_NAME, FUNCTIONAL_PROCESS_TYPE, FUNCTIONAL_PROCESS_SUBTYPE, ACTIVITY_CENTER_NL, ACTIVITY_CENTER_FR, ACTIVITY_CENTER_DE, PARENT_ACTIVITY_CENTER_CODE, PARENT_FUNCTIONAL_PROCESS, PARENT_FUNCTIONAL_PROCESS_TYPE, PARENT_ACTIVITY_CENTER, FRC_CODE, ORG_CODE, IS_INTERNAL, IS_REAL, STATUS, STATUS_VALID_FROM, SITE_NAME, OFFICIAL_STREET, OFFICIAL_HOUSE, OFFICIAL_BOX, OFFICIAL_POSTAL_CODE, OFFICIAL_MUNICIPALITY, CONTACT_PERSON, CONTACT_PHONE, CONTACT_FAX, CONTACT_EMAIL, NON_OFFICIAL_LANGUAGE, FIELD29, FIELD30, FIELD31, FIELD32, FIELD33, FIELD34, FIELD35, FIELD36, FIELD37, FIELD38, FIELD39, FIELD40, PROCESS_NAME, ETL_CYCLE) 
            #               VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22, :23, :24, :25, :26, :27, :28, :29, :30, :31, :32, :33, :34, :35, :36, :37, :38, :39, :40)
            # To resolve this, we need to ensure that the number of values we are trying to insert matches the number of columns in the table. We can either - 
            #    1. Add default values - If there are columns in the table that can have default values, modify your INSERT statement to include those defaults.
            #    2. Modify the Source File - Adjust the source file to include the necessary columns.
            #    3. Change the SQL statement - If certain columns are optional, you may omit them from the INSERT statement. Just ensure that you only list the columns present in the source file.
            # Below code creates a new list `stagingTableColumnsModifiedv1` that contains only the columns from `stagingTableColumns` that are NOT marked as optional.
            stagingTableColumnsModifiedv1 = [stagingTableCol.strip() for stagingTableCol in stagingTableColumns if stagingTableCol.strip() not in stagingTableOptionalColumns]

            # We encountered a scenario where the column names of an Oracle table included spaces, leading to the error [missing comma] in the INSERT statement. This error indicates a syntax issue, typically due to a missing comma in the list of fields or values.
            # For example - INSERT INTO CDW_STG.TBL_USR_MISSING_AC(ACTIVITY_CENTER_ID, AC_CODE, DESCRIPTION, Short Name, RC Code, Start of validity, End of validity, Current status, Name NL, Name FR, Name EN, Name DE, Working Area NL, Working Area FR, Working Area EN, Working Area DE, Is Accountable, Is Internal, Is Real, Activity Type EN, Activity Sub Type EN, Is Org) VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22)
            # Solution - In Oracle SQL, any column names that contain spaces must be enclosed in double quotes. 
            # Revised INSERT statement should look like this - INSERT INTO CDW_STG.TBL_USR_MISSING_AC(ACTIVITY_CENTER_ID, AC_CODE, DESCRIPTION, "Short Name", "RC Code", "Start of validity", "End of validity", "Current status", "Name NL", "Name FR", "Name EN", "Name DE", "Working Area NL", "Working Area FR", "Working Area EN", "Working Area DE", "Is Accountable", "Is Internal", "Is Real", "Activity Type EN", "Activity Sub Type EN", "Is Org") VALUES(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15, :16, :17, :18, :19, :20, :21, :22)
            # Below code creates a new list `stagingTableColumnsModifiedv2` where -
            #    1. If a space is found in the column name, it appends the column name to the `stagingTableColumnsModifiedv2` list, wrapped in double quotes.
            #    2. If there are no spaces in the column name, it simply appends the original column name to the `stagingTableColumnsModifiedv2` list.
            # This is useful for preparing column names for SQL queries or other contexts where spaces in names need to be handled appropriately.
            stagingTableColumnsModifiedv2 = []
            for stagingTableCol in stagingTableColumnsModifiedv1:
                if re.search(r'\s', stagingTableCol) is not None:   # If a space is found, re.search returns a match object, which is considered True.
                    stagingTableColumnsModifiedv2.append(f'"{stagingTableCol}"')
                else:
                    stagingTableColumnsModifiedv2.append(stagingTableCol)

            # Following line is crucial. It commits the changes made by the insert operation to the database. Without a commit, the data might not be permanently written.
            connection.commit()
            print(f'--------------The fields contained within the table {staging_tablename} are:--------------\n{stagingTableColumnsModifiedv2}\n')
            return stagingTableColumnsModifiedv2
        except Exception as e:
            print(f'Failed to retrieve the staging table fields - {e}')
        # In database operations, it's crucial to close cursors and connections when you're done with them to avoid resource leaks and potential issues with subsequent database interactions.
        finally:
            if cursor:
                cursor.close()  # Close the database cursor object, releasing resources associated with it.
            if connection:
                connection.close()  # Close the database connection, freeing up resources on the database server.  

    def load_csv_data_into_table(staging_tablename, staging_table_columns, dataDF_columns, modified_dataInsertionTuples, dateColumnConversion):
        try:
            connection = connect_to_oracledb()
            cursor = connection.cursor()
        
            # # Below code sets up a mechanism to handle float values when interacting with an Oracle database by converting any NaN values to None before being inserted, ensuring that these NaN values are treated as NULL in the database.
            # def input_type_handler(cursor, value, arraysize):                
            #     if isinstance(value, float):     
            #         # `oracledb.DB_TYPE_NUMBER` --> Specifies that the variable should be treated as a number type in the Oracle database.
            #         # `inconverter` is a lambda function used to convert incoming values. It checks if `x` is nan (using math.isnan(x)). If it is, it converts x to None (which is generally a way to handle nan values in databases by converting them to NULL). If x is not nan, it returns the value as-is.
            #         return cursor.var(oracledb.DB_TYPE_NUMBER, arraysize=arraysize, 
            #                           inconverter=lambda x: None if math.isnan(x) else x)
            # connection.inputtypehandler = input_type_handler    # Assigns the `input_type_handler` function to the `inputtypehandler` attribute of the connection object. This means that whenever the database connection needs to handle input data (e.g. inserting or updating values), it will use the `input_type_handler` function to process float values.
            
            # Below code efficiently creates a dynamic INSERT query that adapts to the number of columns in the dataframe. 
            # The resulting query includes the target table name, a list of column names and an equal number of placeholders for data binding during insertion. It avoids manually listing column names and placeholders, making it flexible for dataframes with varying structures.
            ## data_insertion_query = f"INSERT INTO {staging_tablename}({', '.join(dataDF_columns[x] for x in range(0, len(dataDF_columns)))}) VALUES({', '.join(':'+str(x+1) for x in range(0, len(dataDF_columns)))})"
            ## print(data_insertion_query)     # Ex - INSERT INTO CDW_STG.STG_CDLBL_TRANSLATE(CODE_LABEL_CODE, CODE_LABEL_TYPE, SOURCE_TABLE, SOURCE_CODE, SOURCE_TYPE) VALUES(:1, :2, :3, :4, :5)
            
            # We encountered the below error which indicates that we are trying to insert a value into the STATUS column of the `TBL_PERSY_PERSYEXTENDED` table that exceeds the maximum allowed size for that column.
            # oracledb.exceptions.DatabaseError: ORA-12899: value too large for column "CDW_STG"."TBL_PERSY_PERSYEXTENDED"."STATUS" (actual: 7, maximum: 6)
            # Solution: Modify the column definition i.e. If the data you're trying to insert is valid and you want to accommodate longer values, you can alter the table to increase the size of the `STATUS` column. - ALTER TABLE CDW_STG.TBL_PERSY_PERSYEXTENDED MODIFY STATUS VARCHAR2(7 CHAR);

            # We encountered a scenario in which the column names of the table and the file were not aligned. 
            # For example - the table `CDW_STG.TBL_USR_CORR_AC_ORGCODE` included columns named `ACTIVITY_CENTER_ID`, `ORG_CODE`, `VALID_FROM`, and `VALID_TO`, whereas the corresponding file contained columns named `AC_CODE`, `ORG_CODE`, `VALID_FROM`, and `VALID_TO`. Here, `AC_CODE` and `ACTIVITY_CENTER_ID` referred to the same data but were labeled differently.
            # Consequently, we had to retract the following line of code --> data_insertion_query = f"INSERT INTO {staging_tablename}({', '.join(dataDF_columns[x] for x in range(0, len(dataDF_columns)))}) VALUES({', '.join(':'+str(x+1) for x in range(0, len(dataDF_columns)))})"
            # This retraction was necessary because we initially assumed that the column names in both the table and the file would align. As a result, we referenced `dataDF_columns` in both places, which contains the column names extracted from the file.
            # data_insertion_query = f"""INSERT INTO {staging_tablename}({', '.join(staging_table_columns[x] for x in range(0, len(staging_table_columns)))}) VALUES({', '.join(':'+str(x+1) for x in range(0, len(dataDF_columns)))})"""
            # print(data_insertion_query)     # Ex - INSERT INTO CDW_STG.TBL_USR_CORR_AC_ORGCODE(ACTIVITY_CENTER_ID, ORG_CODE, VALID_FROM, VALID_TO) VALUES(:1, :2, :3, :4)

            # We encountered an error: `ORA-01843: not a valid month` while executing this INSERT statement --> INSERT INTO CDW_STG.TBL_USR_MISSING_HIERARCHIES(POSTAL_ORG_UNIT_ID_FROM, POSTAL_ORG_UNIT_ID_TO, EFFCTV_DATE_FROM, EFFCTV_DATE_TO, RELATION_TYPE, "LEVEL FROM PARENT") VALUES('abcd-1234', 'pqrs-6789', '01/10/2006', '31/12/2999', 'PARENT-CHILD', 1) 
            # This error typically occurs when the date format of the input string does not match the expected date format in Oracle.
            # Below code snippet is designed to prepare a list of values for an SQL insert statement, specifically accommodating date columns that require conversion using the TO_DATE() function in Oracle.
            # When you explicitly use TO_DATE(), you provide a format model that tells Oracle how to interpret each part of the date string. 
            # If you don't use TO_DATE() and simply provide a date string, Oracle will attempt to interpret it based on the session's `NLS_DATE_FORMAT` setting. This format specifies the default way Oracle expects dates to be provided.
            # Revised INSERT statement --> INSERT INTO CDW_STG.TBL_USR_MISSING_HIERARCHIES(POSTAL_ORG_UNIT_ID_FROM, POSTAL_ORG_UNIT_ID_TO, EFFCTV_DATE_FROM, EFFCTV_DATE_TO, RELATION_TYPE, "LEVEL FROM PARENT") VALUES('abcd-1234', 'pqrs-6789', TO_DATE('01/10/2006', 'DD/MM/YYYY'), TO_DATE('31/12/2999', 'DD/MM/YYYY'), 'PARENT-CHILD', 1) 
            valuesToInsert = []
            for x in range(0, len(dataDF_columns)):
                dateColumnIndexExists = False
                for dateColConversionDict in dateColumnConversion:
                    if x == dateColConversionDict['dateColumnIndex']:
                        dateColumnIndexExists = True
                        valuesToInsert.append(f"""TO_DATE(:{x+1}, '{dateColConversionDict["dateColumnConversionFormat"]}')""")
                if not dateColumnIndexExists:    
                    valuesToInsert.append(f""":{x+1}""")
            valuesToInsert = ', '.join(valuesToInsert)

            data_insertion_query = f"""INSERT INTO {staging_tablename}({', '.join(staging_table_columns[x] for x in range(0, len(staging_table_columns)))}) VALUES({valuesToInsert})"""
            print(data_insertion_query)     # Ex - INSERT INTO CDW_STG.TBL_USR_MISSING_HIERARCHIES(POSTAL_ORG_UNIT_ID_FROM, POSTAL_ORG_UNIT_ID_TO, EFFCTV_DATE_FROM, EFFCTV_DATE_TO, RELATION_TYPE, "LEVEL FROM PARENT") VALUES(:1, :2, TO_DATE(:3, "DD/MM/YYYY"), TO_DATE(:4, "DD/MM/YYYY"), :5, :6)

            # executemany() method of the cursor object is efficient for inserting multiple rows at once. It takes the query and a list of tuples as arguments. Each tuple in the list represents a single row of data to be inserted.
            cursor.executemany(data_insertion_query, modified_dataInsertionTuples)
            # Following line is crucial. It commits the changes made by the insert operation to the database. Without a commit, the data might not be permanently written.
            connection.commit()
            print(f'--------------Data successfully inserted into table {staging_tablename}--------------\n')
        # In database operations, it's crucial to close cursors and connections when you're done with them to avoid resource leaks and potential issues with subsequent database interactions.
        finally:
            if cursor:
                cursor.close()  # Close the database cursor object, releasing resources associated with it.
            if connection:
                connection.close()  # Close the database connection, freeing up resources on the database server.  
            
            
    # Creates a client object to interact with S3 services.
    s3_client = boto3.client('s3')
    
    # Retrieves a list of objects from the specified S3 bucket.
    # Bucket = cdw_vars.s3_bucket --> Specifies the name of the S3 bucket to list objects from.
    # Prefix = 'inputstream/cdw/' --> Helps narrow down the search to a specific directory within the bucket.
    # .get('Contents) --> Extracts the list of object details (files) from the response dictionary returned by list_objects_v2().
    objects_list = s3_client.list_objects_v2(Bucket=cdw_vars.s3_bucket, Prefix='inputstream/cdw/').get('Contents')
    
    for obj in objects_list:
        obj_name = obj['Key']   # Key --> The name that you assign to an object. You can use the object key to retrieve the object.
        
        dataDF_columns = []   # The list will be populated with column names as they are identified or processed.
        dataInsertionTuples = []    # The list will be populated with tuples as data is processed or extracted.
        
        # The entire condition checks if obj_name ends with either '.csv' or '.xlsx' and if obj_name is equal to source_filepath. If both of these conditions are True, the whole expression evaluates to True.
        if (obj_name.endswith('.csv') or obj_name.endswith('.xlsx')) and obj_name == source_filepath:
            response = s3_client.get_object(Bucket=cdw_vars.s3_bucket, Key=obj_name)    # get_object() --> Retrieves an object from Amazon S3
            
            if obj_name.endswith('.csv'):
                # response['Body] --> Accesses the response body which contains the file content.
                # .read() --> Reads the entire content of the body into a byte stream.
                # .decode('utf-8') --> Decodes the byte stream into a string assuming the CSV file uses utf-8 encoding.
                object_content = response['Body'].read().decode('utf-8')
                
                # Reads the decoded content (CSV data) into a pandas dataframe using pd.read_csv()
                # StringIO(object_content) --> Creates an in-memory file-like object from the decoded string (CSV data).
                dataDF = pd.read_csv(StringIO(object_content), delimiter=';', low_memory=False)
                
                # Extracts the column names from a pandas df and creates a regular python list.
                dataDF_columns = dataDF.columns.values.tolist()
                
                # Convert dataframe to list of tuples.  df.values --> Converts dataframe into list of lists and each list item will be a row. However, instead of list of lists we need list of tuples.
                dataInsertionTuples = [tuple(x) for x in dataDF.values]
            
            elif obj_name.endswith('.xlsx'):
                # response['Body] --> Accesses the response body which contains the file content.
                # .read() --> Reads the entire content of the body into a byte stream.
                object_content = response['Body'].read()
                
                # Load the excel file into a pandas dataframe.
                dataDF = pd.read_excel(BytesIO(object_content), engine = 'openpyxl')     # Use the 'openpyxl' library as the engine to handle the '.xlsx' file format
            
                # Extracts the column names from a pandas df and creates a regular python list.
                dataDF_columns = dataDF.columns.values.tolist()
            
                # Iterates over each row in a pandas DataFrame named `dataDF` and appends the row data as a tuple to a list called `dataInsertionTuples`.
                for _, rowInDataDF in dataDF.iterrows():
                    dataInsertionTuples.append(tuple(rowInDataDF))
            
            # Iterate through a list of tuples, converting any NaN float values to None. 
            # This is useful in scenarios where `NaN` values need to be represented as `None (or NULL)` before inserting or using the data in a database or other systems where `NaN` is not an acceptable value.
            modified_dataInsertionTuples = []
            for dataTuple in dataInsertionTuples:
                dataList = list(dataTuple)  # This is necessary because tuples are immutable (cannot be changed), but lists are mutable (can be modified).
                for idx, value in enumerate(list(dataList)):
                    if isinstance(value, float):
                        if math.isnan(value):
                            dataList[idx] = None
                modified_dataTuple = tuple(dataList)
                modified_dataInsertionTuples.append(modified_dataTuple)
            print(f'DataFrame_Columns =>\n{dataDF_columns}', end='\n\n')
            print(f'Modified_DataInsertion_Tuples =>\n{modified_dataInsertionTuples}', end='\n\n')
            
            delete_existing_data_before_load(staging_tablename)   # Deletes existing data from a staging table before loading new data.

            validate_by_count_query(staging_tablename)      # Executes a count query on the staging table to potentially verify count of records before and after insertion.
            
            staging_table_columns = retrieve_staging_table_column_names(staging_tablename, stagingTableOptionalColumns)      # Fetches the names of the columns in a specified database table.

            load_csv_data_into_table(staging_tablename, staging_table_columns, dataDF_columns, modified_dataInsertionTuples, dateColumnConversion)        # Handles the actual insertion of data into the database table.

            validate_by_count_query(staging_tablename)

# Note - To call a function in Python while skipping a parameter, you can use keyword arguments. This allows you to specify which arguments you are providing values for, without having to follow the positional order. 
#        Using keyword arguments is an effective way to call functions when you want to skip certain parameters without affecting the order of the others. Just ensure that the parameters you want to skip have default values defined in the function signature.
