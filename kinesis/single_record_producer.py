import boto3
import json
from datetime import datetime
import calendar
import random
import time
import sys

stream_name = 'aggstream1'

kinesis_client = boto3.client('kinesis', region_name='us-east-1')

def put_to_stream(symbol, price, ticker_timestamp):
    payload = {
                'symbol': str(symbol),
                'price': price,
                'timestamp': str(ticker_timestamp)
              }

    #print(payload)
    print("Sending these number of records to the Kinesis stream: 1")
    print("Sending these number of bytes to the Kinesis stream: " + str(sys.getsizeof(payload)))
    put_response = kinesis_client.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(payload),
                        PartitionKey=symbol)
ctr=0

price = random.uniform(10.5, 500.5)
ticker_timestamp = calendar.timegm(datetime.utcnow().timetuple())
symbols = ['AAPL','AMZN','MSFT', 'FB', 'MCD', 'SBUX']
symbol = random.choice(symbols)
put_to_stream(symbol, price, ticker_timestamp)
ctr=ctr+1

    # wait for 5 second
    #time.sleep(1)


