# You can create more variables according to your project. The following are the basic variables that have been provided to you
DB_PATH = '/home/airflow/dags/lead_scoring_data_pipeline/'
DB_FILE_NAME = 'lead_scoring_data_cleaning.db'
UNIT_TEST_DB_FILE_NAME = 'unit_test_cases.db'
DATA_DIRECTORY = '/home/airflow/dags/lead_scoring_data_pipeline/data/'
INTERACTION_MAPPING = '/home/airflow/dags/lead_scoring_data_pipeline/mapping/'
INDEX_COLUMNS_TRAINING = ['created_date', 'city_tier', 'first_platform_c', 'first_utm_medium_c','first_utm_source_c', 'total_leads_droppped', 'referred_lead','app_complete_flag']  
INDEX_COLUMNS_INFERENCE = ['created_date', 'city_tier', 'first_platform_c', 'first_utm_medium_c','first_utm_source_c', 'total_leads_droppped', 'referred_lead']  
NOT_FEATURES = ['created_date']
