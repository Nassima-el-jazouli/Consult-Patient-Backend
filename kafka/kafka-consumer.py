import threading
from kafka import KafkaConsumer
from json import loads
import json
import pandas as pd
#Pregnancies,Glucose,BloodPressure,SkinThickness,Insulin,BMI,DiabetesPedigreeFunction,Age,Outcome
lst = ['Pregnancies', 'Glucose', 'BloodPressure', 'SkinThickness', 
            'Insulin', 'BMI', 'DiabetesPedigreeFunction','Age','Outcome']
df = pd.DataFrame(columns = ['preg', 'plas', 'pres','skin','insu','mass','pedi','age','class'])

def get_data():
    consumer = KafkaConsumer('Topic', bootstrap_servers=['localhost:9092']
    , auto_offset_reset='earliest'
                         )

    for message in consumer:
        global df
  
        sliced_row=message.value.decode().split(",")
        try:
            df.loc[len(df)] = sliced_row
            print(df.describe())
        except:
            print(df.describe())
get_data()