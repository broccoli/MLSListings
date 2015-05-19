

import httplib2
import json
import re
import time

import MySQLdb as mdb   #@UnresolvedImport


"""
basic url format.  Must indicate sensor value
http://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&sensor=true_or_false
"""

#address = '11620 Timber Heights Dr, Austin, TX'
ADDRESS = '4209 Burnet Rd, Austin, TX'

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

def google_geocoder_tester():

    #address = '11620 Timber Heights Dr, Austin, TX'
    address = '4209 Burnet Rd, Austin, TX'
    address = re.sub(' +', '+', address)
    base_url = "http://maps.googleapis.com/maps/api/geocode/json?address=my_address&sensor=false"
    url = base_url.replace('my_address', address)
    resp, content = httplib2.Http().request(url)
    
    result = GoogleResult(resp, content)
    print result
    
    print content
#    print resp
#    print content
#    #resp_d = json.loads(resp)
#    content_d = json.loads(content)
#    #print resp_d
#    print content_d
#    print 
#
#    for item in content_d['results'][0]['address_components']:
#        if 'neighborhood' in item['types']:
#            print 'vvvvvvvvvvvv', item
#        elif 'postal_code' in item['types']:
#            print 'mmmmmmmmmm', item

#google_geocoder_tester()

        
        

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
        
    """
    # replace spaces with plusses for valid Google geocoder request
    address = re.sub(' +', '+', address)
    base_url = "http://maps.googleapis.com/maps/api/geocode/json?address=my_address&sensor=false"
    url = base_url.replace('my_address', address)
    resp, content = httplib2.Http().request(url)
    
    return GoogleResult(resp, content)


def update_google_data(pk, google_data, table):
    """
    Update a given table with geocode data.  The data is contained
    in a GoogleResult object.
    """
    
    def nullify(value):
        if value:
            return value
        else:
            return ''
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor()

    query = """ UPDATE {0} SET latitude=%s, longitude=%s, zip=%s, neighborhood=%s, geocode_status=%s
        WHERE id=%s """.format(table)
    print query
    data = []
    data.append(google_data.latitude)
    data.append(google_data.longitude)
    data.append(google_data.zip)
    data.append(google_data.neighborhood)
    data.append(google_data.google_status)
    data.append(pk)
    cur.execute(query, data)
    
    cur.close()
    con.commit()
    con.close()
    
    print "************ done update_lat_and_long ****************"

def update_lease_records_with_geocodes():
    """
    Select records and geocode    
    """
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    #cur = con.cursor()
    query = """ SELECT id, address FROM austin_lease where geocode_status IS NULL """
    cur.execute(query)
    recs = cur.fetchall()
    cur.close()
    con.close()
    
    # geocode records
    print recs
    for rec in recs:

        #Check if there's an address.  Not all of the search results show an address        
        if rec['address']:
            address = ', '.join([rec['address'], 'Austin', 'TX'])
            
            #get GoogleResult object
            google_data = get_google_data(address)
            
            #update table with data
            update_google_data(pk=rec['id'], google_data=google_data, table="austin_lease")
            
            #sleep to avoid Google Geocode Api error
            time.sleep(2)
            
            print rec
    
    
    print "************ done UPDATE RECORDS WITH GEOCODES ****************"

#update_lease_records_with_geocodes()

def update_google_data_for_sale(pk, google_data, table):
    """
    This method is the same as update google data, except it does not update the zip.
    Use for austin_sale when the zip code is already filled.
    """
    
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor()

    query = """ UPDATE {0} SET latitude=%s, longitude=%s, neighborhood=%s, geocode_status=%s
        WHERE id=%s """.format(table)
    print query
    data = []
    data.append(google_data.latitude)
    data.append(google_data.longitude)
    data.append(google_data.neighborhood)
    data.append(google_data.google_status)
    data.append(pk)
    cur.execute(query, data)
    
    cur.close()
    con.commit()
    con.close()
    
    print "************ done update_lat_and_long ****************"

def update_sales_records_from_txt():

    # read file and get list of mls numbers
    
    # get austin_sale record for mls
    
    # get google data for address
    
    # update table with google data
    
    f = open('html_source/sale_to_geocode.txt')
    lines = f.read().split('\n')
    
    for line in lines:
        if line:
            pass

            print 'mls:', line
            con = mdb.connect('localhost', 'root', 'password', 'submarine');
            cur = con.cursor(mdb.cursors.DictCursor)
            #cur = con.cursor()
            query = """ SELECT id, address, geocode_status FROM austin_sale where mls = %s """
            cur.execute(query, [line])
            rec = cur.fetchone()
            cur.close()
            con.close()
            
            print rec
            
            if rec and not rec['geocode_status']:

                google_data = get_google_data(rec['address'])
            
                update_google_data_for_sale(pk=rec['id'], google_data=google_data, table="austin_sale")
            
            #sleep to avoid Google Geocode Api error
            time.sleep(2)
            
    print 'done updating sales records with geocode'

update_sales_records_from_txt()


