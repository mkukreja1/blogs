import json
from datetime import datetime
import calendar
import random
import time
import argparse
import asyncio
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData
async def run():
 parser = argparse.ArgumentParser()
 parser.add_argument('--EVENTHUB_NAME', dest='EVENTHUB_NAME')
 parser.add_argument('--EVENTHUB_CONN_STR', dest='EVENTHUB_CONN_STR')
 args = parser.parse_args()
 # print(args)
 EVENTHUB_CONN_STR=args.EVENTHUB_CONN_STR
 EVENTHUB_NAME=args.EVENTHUB_NAME
 producer = EventHubProducerClient.from_connection_string(conn_str=EVENTHUB_CONN_STR, eventhub_name=EVENTHUB_NAME)
 async with producer:
 # Create a batch.
  event_data_batch = await producer.create_batch()
  terminalvoltage = round(random.uniform(43.5, 46.5), 2)
  #print(terminalvoltage)
  batterypack_timestamp = calendar.timegm(datetime.utcnow().timetuple())
  batterypacks = ['Scooter1','Scooter2', 'Scooter3', 'Scooter4', 'Scooter5']
  batterypack = random.choice(batterypacks)
  payload = {
                'id'         : 1,
                'eventType'  : 'battery',
                'subject'    : 'iot/batterysensors',
                'eventTime'  : str(batterypack_timestamp),
                'data'       : { 'Timestamp' : batterypack_timestamp, 'Battery Pack': batterypack, 'Terminal Voltage': terminalvoltage,
                                 'Charging Current' : 0, 'Discharging Current' : 0, 'SoC' : 1.6, 'Charge Capacity' : 0,
                                 'Charging Power' : 0, 'Discharging Power' : 0, 'Cycle Count' : 0
                               },
                'dataVersion': '1.0'
              }
 # Add events to the batch.
  event_data_batch.add(EventData(json.dumps(payload)))
 # Send the batch of events to the event hub.
  await producer.send_batch(event_data_batch)
loop = asyncio.get_event_loop()
loop.run_until_complete(run())