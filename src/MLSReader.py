
import re
import csv

import MySQLdb as mdb   #@UnresolvedImport

from datetime import datetime

from BeautifulSoup import BeautifulSoup


now = datetime.now()
datef = now.strftime("%Y-%m-%d")

PROPERTY_TYPE = 'house'     #duplex, house, condo, apartment
LIST_TYPE = 'purchase' #lease or purchase
REGION = 'central'
#REGIONS = ['southwest']
REGIONS = ['central', 'east', 'north_central', 'northwest', 'south', 'southeast', 'southwest', 'west']

#file_name = '{0}_{1}_{2}.txt'.format(LIST_TYPE, PROPERTY_TYPE, REGION)

class MLSLease():
    
    """
    This object holds data for Actris lease listings from Mlxchange.  The data
    can be saved in a database
    """
    #==============================================================================
    # The desired data is within a <b> tag following the name of the data. So,
    # Find the text with re, go up to the parent, then down to the <b> tag, and grab the string.
    # <td colspan="1">
    # MLS #:&nbsp;
    # <b>
    #  7484804
    # </b>
    # </td>
    #==============================================================================
    
    def __init__(self, bs_table, region, property_type):
        self.region = region
        self.property_type = property_type
        self.bs_table = bs_table
        self.mls = bs_table.findNext(text=re.compile('MLS')).parent.findNext('b').string
        self.address = bs_table.findNext(text=re.compile('Address')).parent.findNext('b').string
        if self.address:
            self.address = re.sub(' +', ' ', self.address)
            self.address = re.sub('#\w*', '', self.address) # get rid of unit/apt numbers
        self.beds_str = bs_table.findNext(text=re.compile('Beds')).parent.findNext('b').string
        self.bath_str = bs_table.findNext(text=re.compile('FB')).parent.findNext('b').string
        self.unit = bs_table.findNext(text=re.compile('Unit')).parent.findNext('b').string        
        self.sf_str = bs_table.findNext(text=re.compile('SF')).parent.findNext('b').string
        self.list_price_str = bs_table.findNext(text=re.compile('List')).parent.findNext('b').string
        self.beds = int(self.beds_str)
        self.bath = int(self.bath_str)
        self.sf = int(self.sf_str)
        self.list_price = int(self.list_price_str.replace(',', ''))
        self.city = bs_table.findNext(text=re.compile('City')).parent.findNext('b').string
        

    def save(self, con):
        cur = con.cursor()
        
        # Check for existing mls record, then insert
        query = """ SELECT COUNT(*) FROM austin_lease WHERE mls = %s
        """
        data = [self.mls]
        cur.execute(query, data)
        recs = cur.fetchone()
        if recs[0] == 0:
        
            query = """INSERT INTO austin_lease (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
            data = [self.mls, self.property_type, self.region, self.address, 
                    self.unit, self.list_price, self.beds, self.bath, self.sf, datef]
            cur.execute(query, data)
            cur.close()
            if self.address:
                print '**************** added record: ' + self.address + ' ******************'
            else:
                print '******************* added record: no address *******************'

        else:
            print 'skipping duplicate: ', self.mls

        
    def __str__(self):
        
        #print self.bs_table
        data = ['address', 'mls', 'unit', 'list_price', 'city', 'beds', 'bath', 'sf']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])



class MLSPurchase():
    
    """
    This object holds data for Actris for sale listings from Mlxchange.  The data
    can be saved in a database.  In the init, pass a Beautiful Soup table containing
    the property info.
    """
    #==============================================================================
    # The desired data is within a <b> tag following the name of the data. So,
    # Find the text with re, go up to the parent, then down to the <b> tag, and grab the string.
    # <td colspan="1">
    # MLS #:&nbsp;
    # <b>
    #  7484804
    # </b>
    # </td>
    #==============================================================================
    
    def __init__(self, bs_table, region, property_type):
        self.bs_table = bs_table
        self.mls = bs_table.findNext(text=re.compile('MLS')).parent.findNext('b').string
        self.property_type = property_type
        self.region = region
        self.address = bs_table.findNext(text=re.compile('Address')).parent.findNext('b').string
        if self.address:
            self.address = re.sub(' +', ' ', self.address)
        self.beds_str = bs_table.findNext(text=re.compile('Beds')).parent.findNext('b').string
        
        self.bath_str = bs_table.findNext(text=re.compile('Bath')).parent.findNext('b').string
        self.unit = bs_table.findNext(text=re.compile('Unit')).parent.findNext('b').string        
        self.sf_str = bs_table.findNext(text=re.compile('SqFt')).parent.findNext('b').string
        self.sf = int(self.sf_str)
        self.list_price_str = bs_table.findNext(text=re.compile('List')).parent.findNext('b').string
        self.list_price = int(self.list_price_str.replace(',', ''))
        self.beds = int(self.beds_str)
        self.bath = int(self.bath_str)
        
        self.year_built_str = bs_table.findNext(text=re.compile('Yr')).parent.findNext('b').string
        self.year_built = int(self.year_built_str)
        self.zip_str = bs_table.findNext(text=re.compile('Zip')).parent.findNext('b').string
        self.zip = int(self.zip_str)
        self.school_dist = bs_table.findNext(text=re.compile('School')).parent.findNext('b').string        

        self.csv_headers = ['list_price', 'mls', 'address', 'region', 'year_built', 'beds', 'bath']

    def save(self, con):
        cur = con.cursor()    
        query = """INSERT INTO austin_sale (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added,
            year_built, school_dist, zip)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
        data = [self.mls, self.property_type, self.region, self.address, 
                self.unit, self.list_price, self.beds, self.bath, self.sf, datef, self.year_built, self.school_dist, self.zip]
        cur.execute(query, data)
        cur.close()
        if self.address:
            print '**************** added record: ' + self.address + ' ******************'
        else:
            print '******************* added record: no address *******************'
        
    def csv_list(self):
        items = [getattr(self, item) for item in self.csv_headers]
        #items = [self.list_price, self.mls, self.address, self.region, self.year_built, self.beds, self.bath]
        
        return items
        
    def __str__(self):
        
        #print self.bs_table
        data = ['address', 'mls', 'list_price', 'region', 'beds', 'bath', 'sf']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])


class MLXentry():
    
    """
    This object holds data for Actris for sale listings from Mlxchange.  The data
    can be saved in a database.  In the init, pass a Beautiful Soup table containing
    the property info.
    """
    #==============================================================================
    # The desired data is within a <b> tag following the name of the data. So,
    # Find the text with re, go up to the parent, then down to the <b> tag, and grab the string.
    # <td colspan="1">
    # MLS #:&nbsp;
    # <b>
    #  7484804
    # </b>
    # </td>
    #==============================================================================
    
    def __init__(self, bs_table, region, property_type):
        
        # COMMON FIELDS
        self.bs_table = bs_table
        self.mls = bs_table.findNext(text=re.compile('MLS')).parent.findNext('b').string
        self.property_type = property_type
        self.region = region
        self.address = bs_table.findNext(text=re.compile('Address')).parent.findNext('b').string
        if self.address:
            self.address = re.sub(' +', ' ', self.address)
            self.address = re.sub('#\w*', '', self.address) # get rid of unit/apt numbers
            
        self.beds_str = bs_table.findNext(text=re.compile('Beds')).parent.findNext('b').string
        
        if property_type == 'lease':    
            self.bath_str = bs_table.findNext(text=re.compile('FB')).parent.findNext('b').string
        elif property_type == 'purchase_house':
            pass
        # unit
        # list price
        # beds
        # bath
        # sf
        # date added
        
        # SALE
        # year_built
        # zip
        # school district
        
        
        self.beds_str = bs_table.findNext(text=re.compile('Beds')).parent.findNext('b').string
        self.bath_str = bs_table.findNext(text=re.compile('Bath')).parent.findNext('b').string
        self.unit = bs_table.findNext(text=re.compile('Unit')).parent.findNext('b').string        
        self.sf_str = bs_table.findNext(text=re.compile('SqFt')).parent.findNext('b').string
        self.sf = int(self.sf_str)
        self.list_price_str = bs_table.findNext(text=re.compile('List')).parent.findNext('b').string
        self.list_price = int(self.list_price_str.replace(',', ''))
        self.beds = int(self.beds_str)
        self.bath = int(self.bath_str)
        
        self.year_built_str = bs_table.findNext(text=re.compile('Yr')).parent.findNext('b').string
        self.year_built = int(self.year_built_str)
        self.zip_str = bs_table.findNext(text=re.compile('Zip')).parent.findNext('b').string
        self.zip = int(self.zip_str)
        self.school_dist = bs_table.findNext(text=re.compile('School')).parent.findNext('b').string        

        self.csv_headers = ['list_price', 'mls', 'address', 'region', 'year_built', 'beds', 'bath']

    def save(self, con):
        cur = con.cursor()
        query = """INSERT INTO austin_sale (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added,
            year_built, school_dist, zip)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
        data = [self.mls, self.property_type, self.region, self.address, 
                self.unit, self.list_price, self.beds, self.bath, self.sf, datef, self.year_built, self.school_dist, self.zip]
        cur.execute(query, data)
        cur.close()
        if self.address:
            print '**************** added record: ' + self.address + ' ******************'
        else:
            print '******************* added record: no address *******************'
        
    def csv_list(self):
        items = [getattr(self, item) for item in self.csv_headers]
        #items = [self.list_price, self.mls, self.address, self.region, self.year_built, self.beds, self.bath]
        
        return items
        
    def __str__(self):
        
        #print self.bs_table
        data = ['address', 'mls', 'list_price', 'region', 'beds', 'bath', 'sf']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])





def get_mls(list_type, regions):

    """
    The function returns a list of objects, either MLSPurchase or MLSLease.
    The function reads html documents and retrieves tables containing the data with Beautiful Soup.
    The Beautiful Soup tables are passed to the constructors.
    
    The function will read all the html files for a given list of regions and a given type
    of listing -- sale or lease.    
    """

    # keep list of mls numbers to check for duplicates.  I tried to get only the lowest
    # level of nested table with Bueautiful Soup, but I'm getting occasional sets of 
    # nested tables, that is, the data is embedded in two levels of tables so it's
    # getting loaded twice.
    mls_numbers = []
    results = []
    
    
    # get list of filenames
    
    for region in regions:
            
    
        # get the file name and path; read the file
        fname = '{0}_{1}_{2}.txt'.format(LIST_TYPE, PROPERTY_TYPE, region)
        fpath = 'html_source/{0}'.format(fname)
        f = open(fpath)
        text = f.read()
        
        soup = BeautifulSoup(text)
        
        scripts = soup.findAll('script')
        [script.extract() for script in scripts]
        imgs = soup.findAll('img')
        [img.extract() for img in imgs]
        
        
        # Get the property tables from the html.  However, some are nested in outer property tables
        # so make a list of tables that do not have property tables as children.
        property_tables_super = soup.findAll('table', 'PropertyDetailGridTable')
        property_tables = []
        for table in property_tables_super:
            subtable = table.find('table', 'PropertyDetailGridTable')
            if not subtable:
                property_tables.append(table)
        
        
        
        for table in property_tables:
            
            #print table.prettify()
            try:
                if list_type == 'lease':
                    entry = MLSLease(bs_table=table, property_type=PROPERTY_TYPE, region=region)
                elif list_type == 'purchase':
                    entry = MLSPurchase(bs_table=table, property_type=PROPERTY_TYPE, region=region)
                    
            except:
                print table.prettify()
                raise
            
            if not entry.mls in mls_numbers:
                #print(entry)
                mls_numbers.append(entry.mls)
                results.append(entry)


    return results

def save_records(list_type, regions):
    
    print 'starting save records'
            
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    
    entries = get_mls(list_type, regions)
    
    for entry in entries:
        entry.save(con)
        
    con.commit()
    con.close()
        
    print '************************ DONE ******************'


def write_csv(list_type, source_file=None):
    
    entries = get_mls(list_type)
    
    with open('output/purchase_condo.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        
        # write header row with csv_list from first entry
        writer.writerow(entries[0].csv_headers)
        
        for entry in entries:
            writer.writerow(entry.csv_list())
            
    print '************** DONE ***********'


save_records(LIST_TYPE, REGIONS)

#write_csv(LIST_TYPE)



#save_records(list_type = LIST_TYPE)

#entries = get_mls(LIST_TYPE)
#
#for entry in entries:
#    print entry








