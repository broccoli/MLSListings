
import re
import csv

import MySQLdb as mdb   #@UnresolvedImport

from datetime import datetime

from BeautifulSoup import BeautifulSoup


now = datetime.now()
datef = now.strftime("%Y-%m-%d")


#===============================================================================
# This module is for parsing property listings from zip realty html.  I am using it 
# to get a csv file of condo listings for Central and East Austin.  I am using zip 
# rather than mlexchange because mlxchange East region goes farther north and east
# than zip, giving properties that are not really centrally located.
#===============================================================================

#=============================================================================
# The property information is in a div with the following html. Most of the fields are not flagged with 
# classes, so they are determined by the order of appearance.  The data for those fields are put in
# strong tags, so just get a list of data in the strong tags.
#
# <div class="hb-addrbox" id="hbaddrbox0">
# <a class="empTextXXL" href="/property/54-Rainey-St-Austin-TX-78701/84092272/detail">
#  <strong>
#   54 Rainey St #1109 #1109
#  </strong>
# </a>
# <div onclick="SearchFormTipBubble.show(this, newTooltipBody)" class="newIcon tooltipImage" id="newlyListedIcon0" title="New - Listed in the last 3 days">
#  NEW
# </div>
# <br />
# <span class="empTextL">
#  Austin - Central, TX 78701
# </span>
# </div>
# <p class="pibox">
# <strong>
#  2
# </strong>
# bed,
# <strong>
#  2
# </strong>
# bath,
# <strong>
#  Condo/Townhouse
# </strong>
# ,
# <strong>
#  1,164
# </strong>
# sq ft,
# <strong>
#  $344
# </strong>
# /sq ft,
# <br />
# <strong>
#  N/A
# </strong>
# sq ft lot, Built:
# <strong>
#  2005
# </strong>
# , On ZipRealty
# <strong>
#  1 days
# </strong>
#=============================================================================





class ZipEntry():
    

    def _convert_to_digits(self, val):
        
        try:
            val = ''.join([i for i in val if i.isdigit()])
            return int(val)
        except:
            return 'NA'
    
    def __init__(self, div):
        
        self.created = datef
        
        self.div = div
        self.address = div.find('a', 'empTextXXL').find('strong').string
        self.city_zip = div.find('span', 'empTextL').string
        self.price_string = div.find('div', 'hb-pricebox').find('span').string
        self.price = int(self.price_string.replace('$', '').replace(',', ''))
        
        pibox = div.find('p', 'pibox')
        strongs = pibox.findAll('strong')
        strong_vals = [strong.string for strong in strongs]
        
        # Fields in the list need to have non-digit characters removed and 
        # be converted into integers.  These fields are:
        # beds, bath, sf, price per sf, year built, days on zip
        
        self.beds = self._convert_to_digits(strong_vals[0])
        self.bath = self._convert_to_digits(strong_vals[1])
        self.type = strong_vals[2]
        self.sf = self._convert_to_digits(strong_vals[3])
        self.price_per_sf = self._convert_to_digits(strong_vals[4])
        self.year_built = self._convert_to_digits(strong_vals[6])
        self.days_on_zip = self._convert_to_digits(strong_vals[7])
        
        
        self.csv_headers = ['price', 'address', 'city_zip', 'beds', 'bath', 'sf', 'price_per_sf', 'year_built', 'days_on_zip', 'created']
        
    def csv_list(self):
        items = [getattr(self, item) for item in self.csv_headers]
        return items
        
    def __str__(self):
        
        #return 'city_zip:{0}; price:{1}'.format([self.city_zip, self.price])
        #return 'city_zip: {0}; price: {1}; strongs: {2}'.format(self.city_zip, self.price, self.strong_vals)
        
        data = ['address', 'city_zip', 'price', 'beds', 'bath', 'type', 'sf', 'price_per_sf', 'year_built', 'days_on_zip']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])

def get_zips():
    
    f = open('html_source/zip_condo_east_central.txt')
    text = f.read()
    soup = BeautifulSoup(text)
    
#    scripts = soup.findAll('script')
#    [script.extract() for script in scripts]
#    imgs = soup.findAll('img')
#    [img.extract() for img in imgs]
    
    #print soup.prettify()
    
    divs = soup.findAll(id=re.compile('homebox'))
    
#    entry = ZipEntry(divs[0])
#    print entry
    
    entries = []
#    
    for div in divs:
        entry = ZipEntry(div)
        entries.append(entry)
        
    return entries


def write_zips_csv():

    entries = get_zips()
    
    with open('output/zip_condos.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        
        # write header row with csv_list from first entry
        writer.writerow(entries[0].csv_headers)
        
        for entry in entries:
            writer.writerow(entry.csv_list())
            
    print '************** DONE ***********'

#write_zips_csv()

def test_zips():
    

    entries = get_zips()
    
    for entry in entries:
        print entry
        print

test_zips()

