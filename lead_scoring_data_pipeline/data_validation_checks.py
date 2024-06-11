#"""
# Import necessary modules
############################################################################## 
# """

import pandas as pd
# from constants import *
# from schema import *
import sqlite3
from sqlite3 import Error
import importlib.util

def module_from_file(module_name, file_path):
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

constants = module_from_file("constants", "/home/airflow/dags/lead_scoring_data_pipeline/constants.py")
schema = module_from_file("schema", "/home/airflow/dags/lead_scoring_data_pipeline/schema.py")

###############################################################################
# Define function to validate raw data's schema
# ############################################################################## 

def raw_data_schema_check():
    '''
    This function check if all the columns mentioned in schema.py are present in
    leadscoring.csv file or not.

   
    INPUTS
        DATA_DIRECTORY : path of the directory where 'leadscoring.csv' 
                        file is present
        raw_data_schema : schema of raw data in the form oa list/tuple as present 
                          in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Raw datas schema is in line with the schema present in schema.py' 
        else prints
        'Raw datas schema is NOT in line with the schema present in schema.py'

    
    SAMPLE USAGE
        raw_data_schema_check
        
    '''
    df_lead_scoring = pd.read_csv(f'{constants.DATA_DIRECTORY}leadscoring.csv',index_col=[0]) # pd.read_csv('data/leadscoring.csv',index_col=[0])
    raw_data_schema = schema.raw_data_schema
    if 'app_complete_flag' in raw_data_schema: raw_data_schema.remove('app_complete_flag')
    input_data_columns = df_lead_scoring.columns.to_list()
    if 'app_complete_flag' in input_data_columns: input_data_columns.remove('app_complete_flag')
    if input_data_columns==raw_data_schema:
        print('Raw datas schema is in line with the schema present in schema.py')
    else:
        print('Raw datas schema is NOT in line with the schema present in schema.py')
   

################################################################################
# Define function to validate model's input schema
# ############################################################################## 

def model_input_schema_check():
    '''
    This function check if all the columns mentioned in model_input_schema in 
    schema.py are present in table named in 'model_input' in db file.

   
    INPUTS
        DB_FILE_NAME : Name of the database file
        DB_PATH : path where the db file should be present
        model_input_schema : schema of models input data in the form oa list/tuple
                          present as in 'schema.py'

    OUTPUT
        If the schema is in line then prints 
        'Models input schema is in line with the schema present in schema.py'
        else prints
        'Models input schema is NOT in line with the schema present in schema.py'
    
    SAMPLE USAGE
        model_input_schema_check
    '''
    connection = sqlite3.connect(constants.DB_PATH + constants.DB_FILE_NAME)
    df_model_input = pd.read_sql('select * from model_input', connection)          
    model_input_schema = schema.model_input_schema
    if 'app_complete_flag' in model_input_schema: model_input_schema.remove('app_complete_flag') 
    
    if (df_model_input.columns==schema.model_input_schema).all:
        print('Models input schema is in line with the schema present in schema.py')
    else:
        print('Models input schema is NOT in line with the schema present in schema.py')
    connection.close()
