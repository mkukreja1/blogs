import boto3
import json
from datetime import datetime
import calendar
import time
from urllib.request import urlopen
api_one = "http://127.0.0.1:8001/hotel/1"
api_two = "http://127.0.0.1:8002/hotel/1"
api_three = "http://127.0.0.1:8003/hotel/1"

stream_name = "hotelStream"
kinesis_client = boto3.client('kinesis',region_name='us-east-1')

def put_to_stream(json):
    put_response = kinesis_client.put_record(StreamName=stream_name,Data=json,PartitionKey="id")
    print(put_response)

def get_json(url):
    response = urlopen(url)
    data=response.read()
    decoded = data.decode("utf-8")
    return decoded.replace("'","\"")
 
while True:
    json_one = get_json(api_one)
    print(json_one)
    put_to_stream(json_one)

    json_two = get_json(api_two)
    print(json_two)
    put_to_stream(json_two)

    json_three = get_json(api_three)
    print(json_three)
    put_to_stream(json_three)

    time.sleep(3600)
