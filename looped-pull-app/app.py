from google.cloud import pubsub_v1
import json
import logging
from collections import defaultdict
import time
import base64

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger()

PROJECT_ID = "dev-3-455613"
SUBSCRIPTION_ID = "looped-pull-log-subscription"

def pull_messages():
    logger.info("-----------Starting the pull function--------------")

    network_dict = defaultdict(int)
    pull_count = 0
    pull_limit = 10000
    max_run_time_secs = 60 
    start_time = time.time()

    with pubsub_v1.SubscriberClient() as subscriber:
        subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

        while True:
            elapsed = time.time() - start_time
            if elapsed > max_run_time_secs:
                logger.info("Reached max execution time. Stopping pull loop.")
                break

            if pull_count > pull_limit:
                logger.info("Pull limit exceeded")
                break

            try:
                response = subscriber.pull(
                    request={
                        "subscription": subscription_path,
                        "max_messages": 1000,
                    },
                    timeout=10 
                )

                ack_ids = []

                if not response.received_messages:
                    logger.info("No more messages in subscription.")
                    break

                for received_message in response.received_messages:
                    try:
                        payload = received_message.message.data.decode('utf-8')
                        log_entry = json.loads(payload)

                        connection_entry = log_entry.get('jsonPayload', {}).get('connection', {})

                        src_ip = connection_entry.get('src_ip')
                        dest_ip = connection_entry.get('dest_ip')
                        dest_port = connection_entry.get('dest_port')
                        src_port = connection_entry.get('src_port')
                        protocol = connection_entry.get('protocol')

                        if not all([src_ip, dest_ip, dest_port, src_port, protocol]):
                            logger.warning("Missing required connection fields. Skipping.")
                            continue

                        key = (src_ip, dest_ip, dest_port, protocol)
                        network_dict[str(key)] += 1
                        pull_count += 1
                        ack_ids.append(received_message.ack_id)

                    except Exception as e:
                        logger.warning(f"Failed to process message: {e}")
                        continue

                # acknowledging successfully processed messages
                if ack_ids:
                    subscriber.acknowledge(
                        request={
                            "subscription": subscription_path,
                            "ack_ids": ack_ids,
                        }
                    )

            except Exception as e:
                logger.error(f"Error pulling messages: {e}")
                break

    duration_ms = round((time.time() - start_time) * 1000, 2)
    logger.info(f"Request arrived at: {start_time}")
    logger.info(f"Total time taken: {duration_ms} ms")
    logger.info(f"Total messages processed: {pull_count}")
    logger.info(f"Network connection summary:{json.dumps(network_dict)}")

if __name__ == "__main__":
    pull_messages()
