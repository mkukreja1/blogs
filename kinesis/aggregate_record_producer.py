import glob
import boto3
import aws_kinesis_agg.aggregator
import time
import uuid
import sys

myStreamName = "aggstream3"

def send_record(agg_record):
    pk, _, data = agg_record.get_contents()
    print("Sending these number of bytes to the Kinesis stream: " + str(sys.getsizeof(data)))
    kinesis_client.put_record(StreamName=myStreamName, Data=data, PartitionKey=pk)

kinesis_client = boto3.client('kinesis', region_name="us-east-1")
kinesis_agg = aws_kinesis_agg.aggregator.RecordAggregator()
kinesis_agg.on_record_complete(send_record)


def main():
    path = 'stocks.json'
    filenames = glob.glob(path)
    #print(filenames)
    for filename in filenames:
        rec_cnt=0 
        with open(filename, 'r', encoding='utf-8') as data:
            pk = str(uuid.uuid4())
            for record in data:
                rec_cnt=rec_cnt+1
                #print(record)
                kinesis_agg.add_user_record(pk, record)
            print("Sending these number of records to the Kinesis stream: " + str(rec_cnt))
        send_record(kinesis_agg.clear_and_get())
        time.sleep(5)
        
main()
