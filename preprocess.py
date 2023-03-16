import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

def preprocess_data(df):
    print("Data Preprocessing Started")
    print(f"Data Shape: {df.shape}")
    
    df.dropna()
    df.drop_duplicates()
    df = df.drop(["timestamp", "transaction_id", "source_bank", "source_bank_city", "destination_bank", "destination_bank_city"], axis = 1)
    df.type = df.type.map({'CASH_IN': 0, 'CASH_OUT': 1, 'DEBIT': 2, 'PAYMENT': 3, 'TRANSFER': 4})
    
    le = LabelEncoder()
    df['source_name'] = le.fit_transform(df['source_name'])
    df['destination_name'] = le.fit_transform(df['destination_name'])
    
    print("Data Preprocessing Completed")
    return df