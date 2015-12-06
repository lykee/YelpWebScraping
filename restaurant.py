'''
# Add YelpAPI key in config.py file
# Input file format 
	Sample :  
		CityName1|Neighbourhood1
		CityName1|Neighbourhood2
		CityName2|Neighbourhood1
		...
		...
# usage: restaurant_original.py [-h] -f FILENAME -s SEARCHTERM

	Scrape Data through Yelp API

	optional arguments:
	  -h, --help            show this help message and exit
	  -f FILENAME, --fileName FILENAME
	                        Name of file containing neighbourhoods and their
	                        respective cities in a pipe-delimited fashion
	  -s SEARCHTERM, --searchTerm SEARCHTERM
	                        Search Category for the data. eg. restaurants, bars,
	                        chinese, etc.

# Output file: formed in 'output' folder

'''


import argparse
from pprint import pprint
import json
import sys
import urllib
import urllib2
import oauth2
import config
import csv
import time
import getlocation
from pymongo.errors import ConnectionFailure
from pymongo import MongoClient


# Global Variables Declaration
API_HOST	= "api.yelp.com"
SEARCH_PATH	= "/v2/search"
BUSINESS_PATH	= "/v2/business/"
# Number of business results to return
SEARCH_LIMIT 	= 5
# Offset the list of returned business results by this amount
OFFSET_LIMIT	= 0
# Sort mode: 0=Best matched (default), 1=Distance, 2=Highest Rated.
SORT_TYPE	= 0
MAX_LIMIT	= 5
TERM		= 'restaurants'
INPUT_FILE_NAME	= ''

# Yelp API Keys from config file
CONSUMER_KEY	= config.consumerKey
CONSUMER_SECRET	= config.consumerSecret
TOKEN		= config.token
TOKEN_SECRET	= config.tokenSecret 


# Request and response from Yelp API
def request(host, path, urlParams=None):
	urlParams = urlParams or {}
	url = 'https://{0}{1}'.format(host, path)
	consumer = oauth2.Consumer(CONSUMER_KEY, CONSUMER_SECRET)
	oauthRequest = oauth2.Request(method="GET", url=url, parameters=urlParams)
	oauthRequest.update(
        	{
	            'oauth_nonce': oauth2.generate_nonce(),
	            'oauth_timestamp': oauth2.generate_timestamp(),
        	    'oauth_token': TOKEN,
	            'oauth_consumer_key': CONSUMER_KEY
	        }
    	)
	token = oauth2.Token(TOKEN, TOKEN_SECRET)
	oauthRequest.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
	signedUrl = oauthRequest.to_url()
	conn = urllib2.urlopen(signedUrl, None)
	
	try:
		response = json.loads(conn.read())
		'''for k,v in response.items():
			print k,v
			if k=='business':
				for i in v:
					print i'''
	finally:
		conn.close()
	
	return response	



# Declare search parameters to be passed to API
def search(location, longitude, latitude):

	urlParams = {
			'location'	: location,
			'cll'		: str(latitude) + ', ' + str(longitude),
			'term'		: TERM,
			'limit'		: SEARCH_LIMIT,
			'offset'	: OFFSET_LIMIT,
			'sort'		: SORT_TYPE
		}
	return request(API_HOST, SEARCH_PATH, urlParams)



def mongoConnect():
	global dbh

	""" Connect to MongoDB """
	try:
		c = MongoClient(host="localhost", port=27017)
		print "Connected successfully"
	except ConnectionFailure, e:
		sys.stderr.write("Could not connect to MongoDB: %s" % e)
		sys.exit(1)
	dbh = c["lykee"]

	

# Read the input file and transform the data -  creates a dictionary with CityName as key and NeighbourhoodNames list as its value
def getallNeighbourhoodData():
	neighbourhood = {}
	with open(INPUT_FILE_NAME, 'r') as f:
        	areas = f.readlines()

	for area in areas:
	        area = area.strip('\n').split('|')
	        if area[0] not in neighbourhood.keys():
	                neighbourhood[area[0]] = list()
	                neighbourhood[area[0]].append(area[1])
	        else:
	                neighbourhood[area[0]].append(area[1])
	return neighbourhood


# Scrape Data for each Neighbourhood
def queryApi(city, neighbourhood = ''):

	global OFFSET_LIMIT
	global MAX_LIMIT

	location = neighbourhood + ', ' + city + ', US'
	# Get longitude and latitude from Google Geocoding API V3 
	longitude, latitude = getlocation.getCoordinates(location)
	
	print location, longitude, latitude

	# Call API twice for each neighbourhood (API response restricted to 20 records for each request) 
	while OFFSET_LIMIT < MAX_LIMIT:
		response = search(location, longitude, latitude)
		MAX_LIMIT = response['total']

		allRestaurantData = response['businesses']
		if len(allRestaurantData) > 0:
			for restaurant in allRestaurantData:
				
				dbh.restaurant.insert(restaurant)#, safe=True)
				
				
	
			time.sleep(4)
			OFFSET_LIMIT += 20
	
	# Write data for each neighbourhood. Maximum of 40 records
	print 'Writing {0} records'.format(OFFSET_LIMIT)
	print dbh.restaurant.find().count()
	
	OFFSET_LIMIT = 0

# Main function
def main():

	parser = argparse.ArgumentParser(description='Scrape Data through Yelp API');
	parser.add_argument('-f', '--fileName', dest='fileName', type=str, help='Name of file containing neighbourhoods and their respective cities in a pipe-delimited fashion', required=True)
	parser.add_argument('-s', '--searchTerm', dest='searchTerm', type=str, help='Search Category for the data. eg. restaurants, bars, chinese, etc. ', required=True)
	inputValues = parser.parse_args()

	global INPUT_FILE_NAME 
	global TERM
	
	mongoConnect()
	INPUT_FILE_NAME = inputValues.fileName
	TERM = inputValues.searchTerm

	allCities = getallNeighbourhoodData()
	
	# For each neighbourhood in each city, get highest rated restaurants 
	for city, neighbourhoods in allCities.items():
		for neighbourhood in neighbourhoods:			
		        try:
				queryApi(city, neighbourhood)
			except urllib2.HTTPError as error:
		                sys.exit('Encountered HTTP error {0}. Abort Program.'.format(error.code))

		# Once all neighbourhoods data for each city is collected, fetch 40 highest rated restaurants in the city. Some restaurants that don't specify neighbourhoods would be skipped in 
		# above API request call
		queryApi(city)

		
if __name__ == '__main__':
	main()
