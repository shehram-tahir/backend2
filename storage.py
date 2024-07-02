# # storage.py
# import json
# import boto3
# from botocore.exceptions import NoCredentialsError, ClientError
#
# s3_client = boto3.client(
#     's3',
#     aws_access_key_id=config['aws_access_key_id'],
#     aws_secret_access_key=config['aws_secret_access_key'],
#     region_name=config['region']
# )
#
# async def get_data_from_storage(lat: float, lng: float):
#     key = f"locations/{lat}_{lng}.json"
#     try:
#         response = s3_client.get_object(Bucket=config['bucket_name'], Key=key)
#         data = response['Body'].read().decode('utf-8')
#         return json.loads(data)
#     except ClientError as e:
#         if e.response['Error']['Code'] == 'NoSuchKey':
#             return None
#         else:
#             raise e
#
# async def store_data(lat: float, lng: float, data):
#     key = f"locations/{lat}_{lng}.json"
#     try:
#         s3_client.put_object(Bucket=config['bucket_name'], Key=key, Body=json.dumps(data))
#     except NoCredentialsError:
#         raise Exception("Credentials not available")

# local_storage.py
import json
import os

from all_types.myapi_dtypes import LocationReq
from config_factory import get_conf

METASTORE_PATH = "Backend/metastore"
CONF = get_conf()
STORAGE_DIR = 'Backend/storage'
os.makedirs(STORAGE_DIR, exist_ok=True)


def get_filename(location_req: LocationReq):
    lat, lng, radius, place_type = location_req.lat, location_req.lng, location_req.radius, location_req.type
    return f"{STORAGE_DIR}/data_{lat}_{lng}_{radius}_{place_type}.json"


async def get_data_from_storage(location_req: LocationReq):
    filename = get_filename(location_req)
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return None

async def get_dataset_from_storage(dataset_id:str):
    filename = f"{STORAGE_DIR}/catalogue_data_{dataset_id}.json"
    if os.path.exists(filename):
        with open(filename, 'r') as file:
            return json.load(file)
    return None

async def store_data(location_req: LocationReq, data):
    filename = get_filename(location_req)
    with open(filename, 'w') as file:
        json.dump(data, file)


async def search_metastore_for_string(string_search):
    meta_file_path = os.path.join(METASTORE_PATH, string_search)
    if os.path.exists(meta_file_path):
        with open(meta_file_path, 'r') as f:
            return json.load(f)
    return None

