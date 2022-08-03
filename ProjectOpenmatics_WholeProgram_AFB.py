# The holistic program to operate with Openmatics data streaming
# The data files are to be locally downloaded, read, and modified
# to be readable in chunks by Power BI and CAN signal decoders

# import gzip
import numpy as np
from pandas import ExcelWriter
import pandas as pd
import os
# import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

# The following line checks on the current version of Azure Storage Blob library
print("Azure Blob Storage v" + __version__)
# Create the BlobServiceClient object which will be used to create a container client
try:
    connect_str = os.getenv('az_storage_conn_str')
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    print("If connection string or authentication has been just made, you are advised to reset the IDE and rerun the code before continuing.")
except Exception as ex:
    print(ex)
    print("You need to set up an Azure Storage connection string as environment variable before continuing")

# Access the AZ_Storage container "csv" for large .csv.gz files, or "odc" for json.gz files
container_name = "csv"
# Instantiate a client to connect to the container
container_client = blob_service_client.get_container_client(container_name)

# The command lists all blobs in the container, generating a list that can be called to download the blob
# ______________________________________________opted out for now______________________________
"""
path = "160"
recursive = False

if not path == '' and not path.endswith('/'):
    path += '/'

blob_iter = container_client.list_blobs()
files = []
for blob in blob_iter:
    relative_path = os.path.relpath(blob.name, path)
    print(relative_path)
    if recursive or not '/' in relative_path:
        files.append(relative_path)

# List the blobs in the container
print("\nListing blobs...")
blob_list = container_client.list_blobs()
for blob in blob_list:
    print("\t" + blob.name)
"""
# ______________________________________________opted out for now______________________________

# Download the designated blob
blob_date = input("(V.1.0) Enter the date in YYYY-MM-DD format\t")
device_id = input("(V.1.1) Enter the last 4 hexadigits of the Device ID\t")
blob_name = "160/"+blob_date+"/7C976350"+device_id+".csv.gz"
# Reassign the last subfolder in AZ-Blob-Storage as the downstream download directory, which is var download_dir
blob_dir = blob_name.split('/')
# Instantiate the client for the chosen blob
blob_client = blob_service_client.get_blob_client(
    container=container_name, blob=blob_name)
download_dir = "C:/Users/ali-fuat.becan/Downloads/Azuredir/" + blob_dir[-2]
# Accepts only the file name in the download file path
download_file_path = os.path.join(download_dir, blob_dir[-1])
print("\nDownloading blob to \n\t" + download_dir)
os.makedirs(download_dir, exist_ok=True)
try:
    with open(download_file_path, "wb") as download_file:
        download_file.write(blob_client.download_blob().readall())
except:
    print("No blob found.")

# Reading, Splitting, and Sorting Large Bulk CSVs
# import ProjectOpMat_CSVCANSplit.py

# try:
    # filenameentry = input("Enter the e-bus data date in YYYY-MM-DD format")
    # filename = download_dir + filenameentry + "/7C97635040CD.csv.gz"
# except:
    # filename = "C:/Users/ali-fuat.becan/Downloads/2021-07-16/7C97635040CD.csv.gz"

# filename = "C:/Users/ali-fuat.becan/Downloads/2021-04-14/7C97635040CD.csv.gz" # download_file_path == filename

# Split the large CSV file form Openmatics into individual chunks
chunk = pd.read_csv(download_file_path, compression='gzip', sep='|', usecols=[
    'dataset', 'IotHubEnqueuedTime', 'ts_msg_usec', 'sample_index', 'timedelta_usec', 'key', 'value'])
# Record the time and date of the first logged data in the CSV chunk
first_timelog = chunk.loc[1, 'IotHubEnqueuedTime']
print(first_timelog + '\n')
# Specify the Data Frame into more significant columns
df = pd.DataFrame(
    chunk, columns=['dataset', 'ts_msg_usec', 'sample_index', 'timedelta_usec', 'key', 'value'])
df1 = df['ts_msg_usec'].unique()
dict = {}
for dataset in df['ts_msg_usec'].unique():
    dict[dataset] = df[df['ts_msg_usec'] == dataset]
    df1 = pd.DataFrame(dict[dataset], columns=[
                       'dataset', 'ts_msg_usec', 'sample_index', 'timedelta_usec', 'key', 'value'])
# use sort_values() to sort multiple columns
df2 = df.sort_values(by=['ts_msg_usec', 'sample_index', 'timedelta_usec'])
# Version 1.1 addition: Combine the timestamps to each other and make a cumulative timelapse from startpoint to endpoint
# for timestamp in df[['ts_msg_usec'] & ['sample_index'] & ['timedelta_usec']]:
#     timestamp = df['ts_msg_usec'] + df['timedelta_usec'] * df['sample_index']
# df2.to_csv('C:\Backup\Test.csv')
print("Now outputting the split chunks for individual CAN channels...")
for key in df['dataset'].unique():
    dw = df2[df2['dataset'] == key].fillna(0)
    dw.to_csv("%s %s.csv" % (download_file_path, key), index=False)
# check the memory usage of every dataframe created, and then close all dataframes to refresh the memory card
print(df.memory_usage())
print('\n')
print(df1.memory_usage())
print('\n')
print(df2.memory_usage())
print('\n')

del(chunk)
del(df)
del(df1)
del(df2)

# Phase II
# A script segment to truncate repeating data

# Use the newly generated files in Power BI next, importing another file that will operate inside Power BI is necessary

# Finally, compress the split and parametrized chunks into .gzip archives, ready to be uploaded back to Azure Storage Blob
