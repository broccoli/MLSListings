


import httplib2
import json
import re
import time


class GoogleResult():
    
    def __init__(self, response, content):
        self.response = response
        self.content = content
        self.response_status = response['status']
        self.google_status = None
        self.longitude = None
        self.latitude = None
        self.zip = None
        self.neighborhood = None
        
        decoded_content = json.loads(content)
        if self.response_status == '200':
            self.google_status = decoded_content['status']
        
        if self.google_status == 'OK':
            
            self.longitude = decoded_content['results'][0]['geometry']['location']['lng']
            self.latitude = decoded_content['results'][0]['geometry']['location']['lat']
            for item in decoded_content['results'][0]['address_components']:
                if 'neighborhood' in item['types']:
                    self.neighborhood = item['short_name']
                elif 'postal_code' in item['types']:
                    self.zip = item['short_name']
            
    def __str__(self):
        data = ['response_status', 'google_status', 'longitude', 'latitude', 'zip', 'neighborhood']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])


def get_google_data(address):
    
    """
    This function gets latitude and longitude from Google maps API.  Response is in json.
    This is a limit of 2500 requests per day.
    Latitude and longitude are return from the json as floats.
    
    Google request status codes:
        OK
        ZERO_RESULTS
        OVER_QUERY_LIMIT
        REQUEST_DENIED
        INVALID_REQUEST
        UNKNOWN_ERROR
        
    sleep for 2 seconds before returning in case the function is being called in a loop.
    """
    # replace spaces with plusses for valid Google geocoder request
    address = re.sub(' +', '+', address)
    base_url = "http://maps.googleapis.com/maps/api/geocode/json?address=my_address&sensor=false"
    url = base_url.replace('my_address', address)
    resp, content = httplib2.Http().request(url)
    
    #sleep to avoid Google Geocode Api error
    time.sleep(2)
    return GoogleResult(resp, content)

