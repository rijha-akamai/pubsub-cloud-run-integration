from google.cloud import pubsub_v1
import json
import logging
from collections import defaultdict
import time
import base64

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#hashmap to store network traffic
network_dict = defaultdict(int)

#counter
pull_count = 0

#time keeper
time_keeper = 0

PROJECT_ID = "dev-3-455613"
SUBSCRIPTION_ID = "pull-log-subscription"

def pull_messages():
    print("running the pull function")
    try:
        start_time = time.time()
        subscriber = pubsub_v1.SubscriberClient()
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

        response = subscriber.pull(
            request={
                "subscription": subscription_path,
                "max_messages": 1000,
            },
            timeout=30
        )

        ack_ids = []
        for received_message in response.received_messages:
            #Processing every log message
            payload = received_message.message.data.decode('utf-8')
            log_entry = json.loads(payload)

            try:
                connection_entry = log_entry['jsonPayload']['connection']

                src_ip = connection_entry.get('src_ip')
                dest_ip = connection_entry.get('dest_ip')
                dest_port = connection_entry.get('dest_port')
                src_port = connection_entry.get('src_port')
                protocol = connection_entry.get('protocol')

                if not src_ip or not dest_ip or not dest_port or not src_port or not protocol:
                    raise ValueError("Missing fields")
        
            except KeyError as e:
                raise KeyError(f"Missing expected fields: {e}")
            

            #creating key for hashmap
            key = (src_ip, dest_ip, dest_port, protocol)
            global network_dict
            network_dict[str(key)] += 1

            #count incrementing
            global pull_count
            pull_count += 1

            
            ack_ids.append(received_message.ack_id)

        if ack_ids:
            subscriber.acknowledge(
                request={
                    "subscription": subscription_path,
                    "ack_ids": ack_ids,
                }
            )
        else:
            print("No messages received.")

        end_time = time.time()
        duration = (end_time - start_time) * 1000

        print(f"time taken to process {len(ack_ids)} : {duration}")
        print(f"Pull Count: {pull_count}")
        print(f"Network connections: {json.dumps(network_dict)}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    pull_messages()
