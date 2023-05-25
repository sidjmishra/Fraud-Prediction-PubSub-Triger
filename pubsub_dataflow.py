# For Data Simulation

from faker import Faker
from faker.providers import DynamicProvider
import random
import numpy as np
import pandas as pd
import datetime
import time
from json import dumps
from google.cloud import pubsub_v1
import string    

from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1

data = []

def fraud_data_generation(n):
    fake = Faker()     
    
    # User ID
    orig = np.random.randint(10, 999999, n)
    dest = np.random.randint(10, 999999, n)
    
    # Amounts
    amounts = np.random.randint(100, 100000, n)
    # oldDest = np.random.randint(0, 10000, n)
    # newDest = np.random.randint(0, 10000, round(n / 4))
    
    for i in range(0, n):
        type_provider = DynamicProvider(
            provider_name = "type",
            elements = ["CASH_IN", "CASH_OUT", "DEBIT", "PAYMENT", "TRANSFER"]
        )
        fake.add_provider(type_provider)
        type_trans = fake.type()
        amount = amounts[i]
        
        # Old Balance Origin
        oldAmount = random.randint(100, 100000)
        if amounts[i] > oldAmount:
            oldAmount = random.sample(range(amounts[i], 100000), 1)[0]
            oldbalanceOrg = oldAmount
        else:
            oldbalanceOrg = oldAmount
        
        # New Balance Origin
        # newAmount = oldOrg[i] - amounts[i]
        # newOrg.append(newAmount)
        if type_trans == "CASH_IN":
            newbalanceOrig = amount + oldbalanceOrg
            oldbalanceDest = 0
        else:
            newbalanceOrig = oldbalanceOrg - amounts[i]
            oldbalanceDest = random.randint(0, 10000)
            # oldbalanceDest = oldDest[i]
            
        
        # New Balance Destination
        if(type_trans != "CASH_IN"):
            bal_provider = DynamicProvider(
                provider_name = "newbalanceDest",
                elements = [0, random.randint(0, 10000), oldbalanceDest + amount, oldbalanceDest]
            )
            fake.add_provider(bal_provider)
            newBal = fake.newbalanceDest()
        else:
            newBal = 0
        
        nameOrig = f"C{orig[i]}"
        nameDest = f"M{dest[i]}"

        # Source Bank
        type_provider = DynamicProvider(
            provider_name = "bank_source",
            elements = ["JPMC", "BOA", "UBS", "MS", "GS", "AE"]
        )
        fake.add_provider(type_provider)
        bank_source = fake.bank_source()
        # Source Bank City
        type_provider = DynamicProvider(
            provider_name = "bank_source_city",
            elements = ["Los Angeles",  "Chicago", "New York", "San Francisco", "Atlanta"]
        )
        fake.add_provider(type_provider)
        bank_source_city = fake.bank_source_city()
        # Destination Bank
        type_provider = DynamicProvider(
            provider_name = "bank_destination",
            elements = ["JPMC", "BOA", "UBS", "MS", "GS", "AE"]
        )
        fake.add_provider(type_provider)
        bank_destination = fake.bank_destination()
        # Destination Bank City
        type_provider = DynamicProvider(
            provider_name = "bank_destination_city",
            elements = ["Los Angeles",  "Chicago", "New York", "San Francisco", "Atlanta"]
        )
        fake.add_provider(type_provider)
        bank_destination_city = fake.bank_destination_city()
        
        # Data payload
        tmp = {
            "timestamp": str(datetime.datetime.now()),
            "transaction_id": "T" 
                + str(''.join(random.choices(string.ascii_uppercase, k = 3))) 
                + str(datetime.datetime.now().timestamp())[0: 4]
                + str(''.join(random.choices(string.ascii_uppercase, k = 2))) 
                + str(datetime.datetime.now().timestamp())[4: 10],
            "step": random.randint(1, 800),
            "type": type_trans,
            "amount": int(amount),
            "source_name": nameOrig,
            "source_bank": bank_source, 
            "source_bank_city": bank_source_city, 
            "oldbalance_source": int(oldbalanceOrg),
            "newbalance_source": int(newbalanceOrig),
            "destination_name": nameDest,
            "destination_bank": bank_destination, 
            "destination_bank_city": bank_destination_city, 
            "oldbalance_destination": int(oldbalanceDest),
            "newbalance_destination": int(newBal),
            "isFlaggedFraud": 0
        }
# source & destination bank name, source & destination location, timestamp
        data.append(tmp)

    return data

def publish_pubsub(message):
    project_id = "PROJECT-ID"
    topic_id = "TOPIC-ID"

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    data_str = dumps(message)
    data_message = data_str.encode("utf-8")
    future = publisher.publish(topic_path, data_message)
    # print(data_message)
    print(future.result())

def recieve_pubsub():
    project_id = "PROJECT-ID"
    subscription_id = "SUBSCRIPTION-ID"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    streaming_pull_future = subscriber.subscribe(subscription_path, callback = callback)
    print(f"Listening for messages ...\n")
    # 'with' block to automatically call close() when done
    with subscriber:
        try:
            streaming_pull_future.result(timeout = 3.0)
        except TimeoutError:
            streaming_pull_future.cancel()  # Trigger the shutdown.
            streaming_pull_future.result()

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {message}.")
    message.ack()

def main():
    print("Generating dummy data")
    m = fraud_data_generation(100)
    print("Publishing pubsub message")
    publish_pubsub(m)
    print(m)
    # for i in m:
        # publish_pubsub(i)
        # time.sleep(2)
    recieve_pubsub()

if __name__ == '__main__':
    main()

# import pandas as pd
# import json

# list = []
# f = open("demo.txt", "r")
# for x in f:
#   list.append(json.loads(x.strip()))

# print(list)
# df = pd.DataFrame.from_dict(list)
# print(df)
