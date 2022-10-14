# -*- coding: utf-8 -*-
"""
Yelp Fusion API code sample.
This program demonstrates the capability of the Yelp Fusion API
by using the Search API to query for businesses by a search term and location,
and the Business API to query additional information about the top result
from the search query.
Please refer to http://www.yelp.com/developers/v3/documentation for the API
documentation.
This program requires the Python requests library, which you can install via:
`pip install -r requirements.txt`.
Sample usage of the program:
`python sample.py --term="bars" --location="San Francisco, CA"`
"""
from __future__ import print_function

import argparse
import json
import pprint
import requests
import sys
import urllib
import boto3
from decimal import *

import datetime
from time import sleep

# This client code can run on Python 2.x or 3.x.  Your imports can be
# simpler if you only need one of those.
try:
    # For Python 3.0 and later
    from urllib.error import HTTPError
    from urllib.parse import quote
    from urllib.parse import urlencode
except ImportError:
    # Fall back to Python 2's urllib2 and urllib
    from urllib2 import HTTPError
    from urllib import quote
    from urllib import urlencode


# Yelp Fusion no longer uses OAuth as of December 7, 2017.
# You no longer need to provide Client ID to fetch Data
# It now uses private keys to authenticate requests (API Key)
# You can find it on
# https://www.yelp.com/developers/v3/manage_app
# Removed KEY
API_KEY= ''


# API constants, you shouldn't have to change these.
API_HOST = 'https://api.yelp.com'
SEARCH_PATH = '/v3/businesses/search'


# Defaults for our simple example.
DEFAULT_TERM = 'indian'
DEFAULT_LOCATION = 'Manhattan'
SEARCH_LIMIT = 50

# Removed KEY
client = boto3.resource(service_name='dynamodb',
                          aws_access_key_id="",
                          aws_secret_access_key="",
                          region_name="us-east-1",
                         )
table = client.Table('yelp-restaurants')


def request(host, path, api_key, url_params=None):
    """Given your API_KEY, send a GET request to the API.
    Args:
        host (str): The domain host of the API.
        path (str): The path of the API after the domain.
        API_KEY (str): Your API Key.
        url_params (dict): An optional set of query parameters in the request.
    Returns:
        dict: The JSON response from the request.
    Raises:
        HTTPError: An error occurs from the HTTP request.
    """
    url_params = url_params or {}
    url = '{0}{1}'.format(host, quote(path.encode('utf8')))
    headers = {
        'Authorization': 'Bearer %s' % api_key,
    }

    print(u'Querying {0} ...'.format(url))

    response = requests.request('GET', url, headers=headers, params=url_params)

    return response.json()


def search(api_key, term, location, offset):
    """Query the Search API by a search term and location.
    Args:
        term (str): The search term passed to the API.
        location (str): The search location passed to the API.
    Returns:
        dict: The JSON response from the request.
    """

    url_params = {
        'term': term.replace(' ', '+'),
        'location': location.replace(' ', '+'),
        'offset': offset,
        'limit': SEARCH_LIMIT
    }
    return request(API_HOST, SEARCH_PATH, api_key, url_params=url_params)

restaurants = {}

def addItems(data, cuisine):
   global restaurants
   with table.batch_writer() as batch:
        for item in data:
            try:
                if item["alias"] in restaurants:
                    continue;
                item["Business ID"] = str(item["id"])
                item["rating"] = Decimal(str(item["rating"]))
                restaurants[item["alias"]] = 0
                item['cuisine'] = cuisine
                item['insertedAtTimestamp'] = str(datetime.datetime.now())
                item["coordinates"]["latitude"] = Decimal(str(item["coordinates"]["latitude"]))
                item["coordinates"]["longitude"] = Decimal(str(item["coordinates"]["longitude"]))
                item['address'] = item['location']['display_address']
                item.pop("distance", None)
                item.pop("location", None)
                item.pop("transactions", None)
                item.pop("display_phone", None)
                item.pop("categories", None)
                if item["phone"] == "":
                    item.pop("phone", None)
                if item["image_url"] == "":
                    item.pop("image_url", None)

                batch.put_item(Item=item)
                sleep(0.001)
            except Exception as e:
                print(e)
                print(item)

def query_api(term, location):
    """Queries the API by the input values from the user.
    Args:
        term (str): The search term to query.
        location (str): The location of the business to query.
    """

    cuisines = ["Indian", "Italian", "Chinese", "Mexican", "Thai", "Japanese"]

    for cuisine in cuisines:
        for i in range(0, 1000, 50):
                response = search(API_KEY, cuisine, location, i)
                addItems(response["businesses"], cuisine)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument('-q', '--term', dest='term', default=DEFAULT_TERM,
                        type=str, help='Search term (default: %(default)s)')
    parser.add_argument('-l', '--location', dest='location',
                        default=DEFAULT_LOCATION, type=str,
                        help='Search location (default: %(default)s)')

    input_values = parser.parse_args()

    try:
        query_api(input_values.term, input_values.location)
    except HTTPError as error:
        sys.exit(
            'Encountered HTTP error {0} on {1}:\n {2}\nAbort program.'.format(
                error.code,
                error.url,
                error.read(),
            )
        )


if __name__ == '__main__':
    main()