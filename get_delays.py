import psycopg2
import pandas as pd
from config import params
import numpy as np

conn = psycopg2.connect(**params)

cursor = conn.cursor()
cursor.execute("SELECT * FROM real_flight WHERE cancelled = '0' and diverted = '0'")
rows = cursor.fetchall()
#for column in cursor.description:
#   print (column.name)
#print (cursor.description)
cursor.close()

df = pd.DataFrame(rows, columns=[column.name for column in cursor.description])
print (df.head())
print (df.isna().sum()) 

df["DELAYED"] = np.where(((df["arr_del15"] == "1") | (df["dep_del15"] == "1")), 1, 0)
print (df["DELAYED"]) 

dfgroup = df.groupby('op_unique_carrier').mean().sort_values(by = "DELAYED", ascending = False)
print (dfgroup)

dfgroup.to_csv("airline_delays.csv") 

dforigin = df.groupby('origin').mean().sort_values(by = "DELAYED", ascending = False)
dforigin.to_csv("airport_delays.csv")
