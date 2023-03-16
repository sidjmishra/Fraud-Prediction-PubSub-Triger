from inferencing import inferencing_record
import pandas as pd

def data_predict(df):
    print("Data Prediction Started")
    
    data_list = df.values.tolist()
    print(f"List Length: {len(data_list)}")
    data_values = []
    
    print("Data Inferencing Started")
    for data in data_list:
        prediction = inferencing_record(data)
        if(prediction == 1.0):
            data.append("Yes")
        else:
            data.append("No")
        # data.append(prediction)
        data_values.append(data)

    predicted_df = pd.DataFrame(
        data_values,
        columns = ["step", "type", "amount", "source_name",
                   "oldbalance_source", "newbalance_source", "destination_name",
                   "oldbalance_destination", "newbalance_destination", "isFlaggedFraud", "isFraud"])
    predicted_df.loc[predicted_df["isFlaggedFraud"] == 1, "isFraud"] = "Yes"
    
    print(f"Data Shape: {predicted_df.shape}")
    print("Data Inferencing Completed")
    
    print("Data Prediction Completed")
    return predicted_df