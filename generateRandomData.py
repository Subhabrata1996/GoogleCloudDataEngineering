import argparse
import os
import sys
import random
from scipy import stats
import datetime
import time
from google.cloud import pubsub_v1

def run(TOPIC_NAME, PROJECT_ID, INTERVAL = 200):
    #Create a Publisher client
	publisher = pubsub_v1.PublisherClient()
    #extract the topic path from Project_id and topic_name
    #projects/<project id>/topics/<topic name>
	topic_path = publisher.topic_path(PROJECT_ID,TOPIC_NAME)
    #Generate random data for 5 presurre sensors
	sensorNames = ['Pressure_1','Pressure_2','Pressure_3','Pressure_4','Pressure_5']
	sensorCenterLines = [1990,2080,2390,1730,1900]
	standardDeviation = [450,400,350,400,370]
	c=0
    #Keep on publishing messages untill there is an explicit interrupt
	while(True):
		for pos in range(0,5):		
			sensor = sensorNames[pos];
            #Generate a random value with corresponding center and standard deviation
			reading = stats.truncnorm.rvs(-1,1,loc = sensorCenterLines[pos], scale = standardDeviation[pos])
            #Get the current timestamp
			timeStamp = str(datetime.datetime.now())
            #form a message in a CSV string format
			message = timeStamp+','+sensor+','+str(reading)
            #Encode and publish the message
			publisher.publish(topic_path, data=message.encode('utf-8'))
			c=c+1
        #Stop execution for a small duration, 200ms by default
        #i.e. every 1 second 25 messages will be published    
		time.sleep(INTERVAL/1000)
		if c == 100:
			print("Published 100 Messages")
			c=0


if __name__ == "__main__":  # noqa
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--TOPIC_NAME",
        help="The Cloud Pub/Sub topic to write to.\n"
        '"<TOPIC_NAME>".',
    )
    parser.add_argument(
        "--PROJECT_ID",
        help="GCP Project ID.\n"
        '"<PROJECT_ID>".',
    )
    parser.add_argument(
        "--INTERVAL",
        type=int,
        default=200,
        help="Interval in mili seconds which will publish messages (default 2 ms).\n"
        '"<INTERVAL>"',
    )
    args = parser.parse_args()
    try:
        run(
        args.TOPIC_NAME,
        args.PROJECT_ID,
        args.INTERVAL
    	)
    #Handler for Keyboard Interrupt    
    except KeyboardInterrupt:
        print('Interrupted : Stopped Publishing messages')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
    