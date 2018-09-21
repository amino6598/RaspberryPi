"""
Python sample for Raspberry Pi which reads temperature and humidity values from
a DHT22 sensor, and sends that data to Power BI for use in a streaming dataset.
"""

import urllib, urllib2, time
from datetime import datetime
import Adafruit_DHT as dht

# type of sensor that we're using
SENSOR = dht.DHT22 	

# pin which reads the temperature and humidity from sensor
PIN = 4			

# REST API endpoint, given to you when you create an API streaming dataset
# Will be of the format: https://api.powerbi.com/beta/<tenant id>/datasets/< dataset id>/rows?key=<key id>
REST_API_URL = "https://api.powerbi.com/beta/69c3b14b-e78e-400a-a3d2-7c0ae48f5375/datasets/1bca8088-9f7b-4509-aa71-952a43aeed8b/rows?key=yQ6ROKfGAYhDK3lJmUAWm3sQ0xmXUdEsSTKFBAGK%2B3094v8K37Ze5FWCXtJNJ8K8Mp%2BBbmUa6FTXa4zgbP89%2BQ%3D%3D"

# Gather temperature and sensor data and push to Power BI REST API
while True:
	try:
		# read and print out humidity and temperature from sensor
		humidity,temp = dht.read_retry(SENSOR, PIN)
		print 'Temp={0:0.1f}*C Humidity={1:0.1f}%'.format(temp, humidity)
		
		# ensure that timestamp string is formatted properly
		now = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S%Z")
	
		# data that we're sending to Power BI REST API
		data = '[{{ "timestamp": "{0}", "temperature": "{1:0.1f}", "humidity": "{2:0.1f}" }}]'.format(now, temp, humidity)
	
		# make HTTP POST request to Power BI REST API
		req = urllib2.Request(REST_API_URL, data)
		response = urllib2.urlopen(req)
		print("POST request to Power BI with data:{0}".format(data))
		print("Response: HTTP {0} {1}\n".format(response.getcode(), response.read()))	
	
		time.sleep(1)
	except urllib2.HTTPError as e:
		print("HTTP Error: {0} - {1}".format(e.code, e.reason))
	except urllib2.URLError as e:
		print("URL Error: {0}".format(e.reason))
	except Exception as e:
		print("General Exception: {0}".format(e))
