import base64
import pandas as pd
import json
from preprocess import preprocess_data
from data_prediction import data_predict
from bigquery_dataset import bigquery_data, insert_df_data, dataset_table

def fraud_detection_trigger(event, context):
    # data_list = []
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    message = json.loads(pubsub_message)
    # data_list.append(message)
    # print(data_list)
    df = pd.DataFrame.from_dict(message, orient = 'columns')
    print(f"Data Shape: {df.shape}")
    print(df)

    # Data Preprocessing
    preprocessed_df = preprocess_data(df)
    print(f"Data Shape: {preprocessed_df.shape}")
    
    # Data Prediction
    predicted_df = data_predict(preprocessed_df)

    # Checking & Creating Dataset
    bigquery_data()
    final_df = merge_data(df, predicted_df)

    # Insert Raw Data
    insert_df_data(df, dataset_table('raw_data'))
    print('Published Raw Data')
    # Inserting Predicted Data
    insert_df_data(final_df, dataset_table('fraud_prediction'))
    print('Published Predicted Data')


def merge_data(original_data, predicted_data):
    original_data.dropna()
    original_data.drop_duplicates()
    final_df = pd.merge(original_data, predicted_data["isFraud"], left_index = True, right_index = True)
    return final_df
