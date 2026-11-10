import random
import uuid
import json
from datetime import datetime, timedelta
from faker import Faker
from azure.eventhub import EventHubProducerClient, EventData
import logging
from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file
import os

# Pulling Data Generator Function
from data import generate_telecom_event

CONNECTION_STRING = os.getenv("CONNECTION_STRING")
EVENT_HUBNAME = os.getenv("EVENT_HUBNAME")


def send_to_event_hub(event_data=None, batch_size=1):

    try:
        # Initialize Event Hub Producer Client
        producer = EventHubProducerClient.from_connection_string(
            CONNECTION_STRING,
            eventhub_name=EVENT_HUBNAME
        )
        
        # Prepare telecom event record
        event_json = json.dumps(event_data, ensure_ascii=False)
        
        # Create batch of events
        event_batch = producer.create_batch()

            
        # Create event with telecom data 
        event = EventData(event_json)
            
        # Add event to batch
        event_batch.add(event)

        # Send batch to Event Hub
        producer.send_batch(event_batch)
        
        producer.close()

        return "Successfully sent to Event Hub"
        
    except Exception as e:
        print(f"Error sending data to Event Hub: {str(e)}")
        return False



if __name__ == "__main__":

    import time

    SEND_INTERVAL_SECONDS = 30   # one event every 10 seconds

    print("=" * 80)
    print(f"STREAMING: 1 event every {SEND_INTERVAL_SECONDS} seconds to Event Hub (press Ctrl+C to stop)")
    print("=" * 80)

    sent_count = 0
    try:
        while True:
            event = generate_telecom_event()
            result = send_to_event_hub(event)
            sent_count += 1
            print(f"[{datetime.now():%H:%M:%S}] #{sent_count} {event['event_type']:<18} "
                  f"event_id={event['event_id']} -> {result}")
            time.sleep(SEND_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        # crtl+c
        print(f"\nStopped. Total events sent: {sent_count}")