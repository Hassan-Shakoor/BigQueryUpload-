from google.cloud import storage
import pandas as pd
import io
from io import BytesIO

storage_client=storage.Client.from_service_account_json("C:/Users/dell/Desktop/task/key.json")
BUCKET_NAME='gcp-training-datasets'
bucket=storage_client.get_bucket(BUCKET_NAME)
filename= list(bucket.list_blobs(prefix=''))

for name in filename:
	print(name.name) # just checking the files in that bucket

blop=bucket.blob("2020_Olympics_Dataset_updated.csv") # accessingt the right file
data=blop.download_as_string()

df=pd.read_csv(io.BytesIO(data), encoding = "ISO-8859-1",sep=",") # reading the file
print(df.head()) # showing data
print(df.columns)

# now creating table in bigqueryt if not exist and populating all data read from csv file using pandas
df.head().to_gbq(destination_table='gcp_training.My_Kaggle_data',project_id='cloud-work-314310',if_exists='fail') 

