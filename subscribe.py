from google.cloud import pubsub_v1
import argparse
import os
import sys


def callback(message):
    print(f"Received {message}.")
    message.ack()

def run(TOPIC_NAME, PROJECT_ID):
	subscriber = pubsub_v1.SubscriberClient()
	subscription_path = subscriber.subscription_path(PROJECT_ID,TOPIC_NAME)
	streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
	print(f"Listening for messages on {subscription_path}..\n")

	with subscriber:
	    try:
	        # When `timeout` is not set, result() will block indefinitely,
	        # unless an exception is encountered first.
	        streaming_pull_future.result()
	    except TimeoutError:
	        streaming_pull_future.cancel()

if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--TOPIC_NAME",
        help="The Cloud Pub/Sub topic to read from.\n"
        '"<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--PROJECT_ID",
        help="GCP Project ID.\n"
        '"<PROJECT_ID>".',
    )
    args = parser.parse_args()
    try:
        run(
        args.TOPIC_NAME,
        args.PROJECT_ID
    	)
    except KeyboardInterrupt:
        print('Interrupted : Stopped Publishing messages')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)