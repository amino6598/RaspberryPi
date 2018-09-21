"""
Python sample for Raspberry Pi which reads temperature and humidity values from
a DHT22 sensor, and sends that data to Power BI for use in a streaming dataset.
"""

import urllib, urllib2, time
from datetime import datetime
import Adafruit_DHT as dht
import iothub_client
from iothub_client import *
from iothub_client_args import *
import sys
import time
from time import gmtime, strftime

# type of sensor that we're using
SENSOR = dht.DHT11 	

# pin which reads the temperature and humidity from sensor
PIN = 4			

# REST API endpoint, given to you when you create an API streaming dataset
# Will be of the format: https://api.powerbi.com/beta/<tenant id>/datasets/< dataset id>/rows?key=<key id>
REST_API_URL = " *** Your Push API URL goes here *** "

# chose HTTP, AMQP or MQTT as transport protocol
protocol = IoTHubTransportProvider.AMQP
connection_string = "HostName=IotAMTI67.azure-devices.net;DeviceId=RaspAM;SharedAccessKey=lNOp2AUOEswyFMEv/NIoT1S3ZZjJKam0ktQxqSHDnwM="

def iothub_client_init():
    # prepare iothub client
    iotHubClient = IoTHubClient(connection_string, protocol)
    if iotHubClient.protocol == IoTHubTransportProvider.HTTP:
        iotHubClient.set_option("timeout", timeout)
        iotHubClient.set_option("MinimumPollingTime", minimum_polling_time)
    # some embedded platforms need certificate information
    # set_certificates(iotHubClient)
    # to enable MQTT logging set to 1
    if iotHubClient.protocol == IoTHubTransportProvider.MQTT:
        iotHubClient.set_option("logtrace", 0)
    return iotHubClient


# Gather temperature and sensor data and push to Power BI REST API
while True:
	try:
		# read and print out humidity and temperature from sensor
		humidity,temp = dht.read_retry(SENSOR, PIN)
		print 'Temp={0:0.1f}*C Humidity={1:0.1f}%'.format(temp, humidity)
		
		# ensure that timestamp string is formatted properly
		now = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S%Z")
	
		#Initate the IoT hub
        iotHubClient = iothub_client_init()
		
		(connection_string, protocol) = get_iothub_opt(sys.argv[1:], connection_string, protocol)
		
		# data that we're sending to Power BI REST API
		data = '[{{ "timestamp": "{0}", "temperature": "{1:0.1f}", "humidity": "{2:0.1f}" }}]'.format(now, temp, humidity)
		
		message = IoTHubMessage(data)
		
		iotHubClient.send_event_async(message)
		
		