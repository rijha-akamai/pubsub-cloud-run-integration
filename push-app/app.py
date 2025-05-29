from flask import Flask, request, jsonify
import base64
import json
import logging
from collections import defaultdict
import time
import threading
import os

logging.basicConfig(level=logging.DEBUG, format="%(levelname)s: %(message)s")
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#hashmap to store network traffic
network_dict = defaultdict(int)

#counter
push_count = 0

time_keeper = 0

app = Flask(__name__)

@app.route("/", methods=["POST"])
def index():
    try:
        start_time = time.time()
        envelope = request.get_json()
        if not envelope:
            return "Bad Request: no JSON", 400

        #Parsing logic
        payload = base64.b64decode(envelope['message']['data']).decode('utf-8')
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
        global push_count
        push_count += 1


        # Process the log entry here
        logging.info("---------new message---------")
        logging.info("push endpoint envoked")
        temp_start_time = start_time * 1000
        logging.info(f"message received at: {temp_start_time} ms")
        #instance_id = os.getenv("CLOUD_RUN_EXECUTION", "unknown")
        #logging.info(f"name of instance: {instance_id}")
        # print("Received log entry:", log_entry)
        end_time = time.time()
        global time_keeper
        time_individual = (end_time - start_time) * 1000
        #logging.info(f"Individual processing time: {time_individual} ms")
        time_keeper += time_individual
        #thread_id = threading.get_ident()
        #logging.info(f"Name of thread: {thread_id}")
        logging.info("---------processing ends----------")
        #time.sleep(0.5)

        return "OK", 200
    
    except Exception as e:
        logging.error(f"Error processing log entry: {e}")
        return "Error processing log entry", 500

@app.route("/push_count", methods=["GET"])
def push_count_endpoint():
    global push_count

    return f"Push Count: {push_count}", 200


@app.route("/network_map", methods=["GET"])
def network_map_endpoint():
    global network_dict
    return jsonify(network_dict)


@app.route("/push_time_taken", methods=["GET"])
def time_taken():
    global time_keeper
    global push_count

    return f"time taken to process {push_count} messages: {time_keeper}", 200


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)
