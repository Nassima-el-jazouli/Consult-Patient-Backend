import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
import time
import random
import numpy as np

# pip install kafka-python

KAFKA_TOPIC_NAME_CONS = "Topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                                       value_serializer=lambda x: x.encode('utf-8'))
    
    filepath = "Diabetes.csv"
    
    dataset = pd.read_csv(filepath)
  
    dataset['Outcome'] = np.arange(len(dataset))

    
    dataset_list = dataset.to_dict(orient="records")
       

    message_list = []
    message = None
    for message in dataset_list:
        
        message_fields_value_list = []
               
        message_fields_value_list.append(message['Pregnancies'])
        message_fields_value_list.append(message['Glucose'])
        message_fields_value_list.append(message['BloodPressure'])
        message_fields_value_list.append(message['SkinThickness'])
        message_fields_value_list.append(message['Insulin'])
        message_fields_value_list.append(message['BMI'])
        message_fields_value_list.append(message['DiabetesPedigreeFunction'])
        message_fields_value_list.append(message['Age'])
        message_fields_value_list.append(message['Outcome'])

        message = ','.join(str(v) for v in message_fields_value_list)
#         print("Message Type: ", type(message))
        print("Message: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(0.01)


    print("Kafka Producer Application Completed. ")