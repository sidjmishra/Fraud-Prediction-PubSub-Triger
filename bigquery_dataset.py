from google.cloud import bigquery
from google.cloud.exceptions import NotFound

def bigquery_data():
    if(check_dataset_exists(client_dataset())): # If returns True - Data does not exists
        project_client().create_dataset(client_dataset(), timeout=30)
        if(check_table_exists(dataset_table('raw_data'))): # If returns True - Table does not exists
            create_table(dataset_table('raw_data'), get_schema(1))
            print("Table Created")
        else:
            print("Table Exists")

        if(check_table_exists(dataset_table('fraud_prediction'))): # If returns True - Table does not exists
            create_table(dataset_table('fraud_prediction'), get_schema(2))
            print("Table Created")
        else:
            print("Table Exists")
    else:
        # Check raw_data table 
        if(check_table_exists(dataset_table('raw_data'))): # If returns True - Table does not exists
            create_table(dataset_table('raw_data'), get_schema(1))
            print("Table Created")
        else:
            print("Table Exists")

        # Check fraud_prediction table
        if(check_table_exists(dataset_table('fraud_prediction'))): # If returns True - Table does not exists
            create_table(dataset_table('fraud_prediction'), get_schema(2))
            print("Table Created")
        else:
            print("Table Exists")

# To Insert Data in BigQuery Table
def insert_df_data(df, table_ref):
    project_client().load_table_from_dataframe(df, table_ref).result()

# Get Google Cloud Porject
def project_client():
    client = bigquery.Client()
    return client

# Get Dataset Name
def client_dataset():
    dataset_ref = project_client().dataset('fraud_prediction_dataset')
    return dataset_ref

# Get Table Name
def dataset_table(table_name):
    table_ref = client_dataset().table(table_name)
    return table_ref

# Check If Dataset Exists
def check_dataset_exists(dataset):
    try:
        project_client().get_dataset(dataset)
        return False
    except NotFound:
        return True

# Check If Table Exists
def check_table_exists(table):
    try:
        project_client().get_table(table)
        return False
    except NotFound:
        return True    

# Define Table Schema
def get_schema(schema):
    if(schema == 1): 
        schema1 = [
            bigquery.SchemaField("timestamp", "STRING"),
            bigquery.SchemaField("transaction_id", "String"),
            bigquery.SchemaField("step", "INTEGER"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("amount", "INTEGER"),
            bigquery.SchemaField("source_name", "STRING"),
            bigquery.SchemaField("source_bank", "STRING"),
            bigquery.SchemaField("source_bank_city", "STRING"),
            bigquery.SchemaField("oldbalance_source", "INTEGER"),
            bigquery.SchemaField("newbalance_source", "INTEGER"),
            bigquery.SchemaField("destination_name", "STRING"),
            bigquery.SchemaField("destination_bank", "STRING"),
            bigquery.SchemaField("destination_bank_city", "STRING"),
            bigquery.SchemaField("oldbalance_destination", "INTEGER"),
            bigquery.SchemaField("newbalance_destination", "INTEGER"),
            bigquery.SchemaField("isFlaggedFraud", "Integer")
        ]
        return schema1
    else:
        schema2 = [
            bigquery.SchemaField("timestamp", "STRING"),
            bigquery.SchemaField("transaction_id", "String"),
            bigquery.SchemaField("step", "INTEGER"),
            bigquery.SchemaField("type", "STRING"),
            bigquery.SchemaField("amount", "INTEGER"),
            bigquery.SchemaField("source_name", "STRING"),
            bigquery.SchemaField("source_bank", "STRING"),
            bigquery.SchemaField("source_bank_city", "STRING"),
            bigquery.SchemaField("oldbalance_source", "INTEGER"),
            bigquery.SchemaField("newbalance_source", "INTEGER"),
            bigquery.SchemaField("destination_name", "STRING"),
            bigquery.SchemaField("destination_bank", "STRING"),
            bigquery.SchemaField("destination_bank_city", "STRING"),
            bigquery.SchemaField("oldbalance_destination", "INTEGER"),
            bigquery.SchemaField("newbalance_destination", "INTEGER"),
            bigquery.SchemaField("isFlaggedFraud", "Integer"),
            bigquery.SchemaField("isFraud", "String"),
        ]
        return schema2

# Create Table
def create_table(table_ref, schema):
    table = bigquery.Table(table_ref, schema = schema)
    project_client().create_table(table)