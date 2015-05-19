

import re

import MySQLdb as mdb   #@UnresolvedImport

from datetime import datetime

from BeautifulSoup import BeautifulSoup

from utils import MLXMultiPurchase, get_property_tables_from_text

now = datetime.now()
datef = now.strftime("%Y-%m-%d")

PLEX = '2plex'  # 2plex, 3plex, 4plex
#REGIONS = ['east']
REGIONS = ['central', 'east', 'north_central', 'northwest', 'south', 'southeast', 'southwest', 'west']

#class MLXMultiPurchase():
#    
#    """
#    This object holds data for Actris for sale listings from Mlxchange.  The data
#    can be saved in a database.  In the init, pass a Beautiful Soup table containing
#    the property info.
#    """
#    #==============================================================================
#    # The desired data is within a <b> tag following the name of the data. So,
#    # Find the text with re, go up to the parent, then down to the <b> tag, and grab the string.
#    # <td colspan="1">
#    # MLS #:&nbsp;
#    # <b>
#    #  7484804
#    # </b>
#    # </td>
#    #==============================================================================
#    
#    def __init__(self, bs_table, plex, region):
#        self.bs_table = bs_table
#        self.plex = plex
#        self.region = region
#        
#        self.mls = bs_table.findNext(text=re.compile('MLS')).parent.findNext('b').string
#        self.address = bs_table.findNext(text=re.compile('Address')).parent.findNext('b').string
#        if self.address:
#            self.address = re.sub(' +', ' ', self.address)
#        self.list_price_str = bs_table.findNext(text=re.compile('List')).parent.findNext('b').string
#        self.list_price = int(self.list_price_str.replace(',', ''))
#        self.rent_total_str = bs_table.findNext(text=re.compile('RentTotal')).parent.findNext('b').string
#        self.rent_total = int(self.rent_total_str)
#        self.school_dist = bs_table.findNext(text=re.compile('Schl')).parent.findNext('b').string        
#        self.stories_str = bs_table.findNext(text=re.compile('Stories')).parent.findNext('b').string
#        self.stories = int(self.stories_str)
#        self.status = bs_table.findNext(text=re.compile('Status')).parent.findNext('b').string        
#        self.year_built_str = bs_table.findNext(text=re.compile('Yr')).parent.findNext('b').string
#        self.year_built = int(self.year_built_str)
#        
#        self.csv_headers = ['list_price', 'mls', 'address', 'region', 'year_built']
#
#    def save(self):
#        
#        # get record if it exists
#        con = mdb.connect('localhost', 'root', 'password', 'submarine');
#        cur = con.cursor(mdb.cursors.DictCursor)
#        query = """ SELECT list_price, status FROM multiplex_sale WHERE mls = %s    
#        """
#        data = [self.mls]
#        cur.execute(query, data)
#        recs = cur.fetchall()
#        cur.close()
#        con.close()
#        
#        if recs:
#            if len(recs) > 1:
#                raise Exception(''.join(['more than one record for', self.mls]))
#            if self.list_price != recs[0]['list_price']:
#                print '******* NEW LIST PRICE FOR {0} *********'.format(self.mls)
#                print '{0} changed to {1}'.format(recs[0]['list_price'], self.list_price)
#            if self.status != recs[0]['status']:
#                print '---------- NEW STATUS FOR {0} ---------'.format(self.mls)
#                print '{0} changed to {1}'.format(recs[0]['status'], self.status)
#            # if status or list price has changed, update both
#            if self.status != recs[0]['status'] or self.list_price != recs[0]['list_price']:
#                print 'updating asdfasdf'
#                con = mdb.connect('localhost', 'root', 'password', 'submarine');
#                cur = con.cursor()
#                query = """ update multiplex_sale set status=%s, list_price=%s WHERE mls = %s """
#                cur.execute(query, (self.status, self.list_price, self.mls))
#                cur.close()
#                con.commit()
#                con.close()
#                
#        else:
#            con = mdb.connect('localhost', 'root', 'password', 'submarine');
#            cur = con.cursor()
#            query = """INSERT INTO multiplex_sale (mls, plex, address, region, list_price, rent_total, school_district, stories, status, year_built, date_added)
#              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
#            data = [self.mls, self.plex, self.address, self.region, 
#                    self.list_price, self.rent_total, self.school_dist, self.stories, self.status, self.year_built, datef]
#            cur.execute(query, data)
#            cur.close()
#            con.commit()
#            con.close()
#            if self.address:
#                print '**************** added record: ' + self.address + ' ******************'
#            else:
#                print '******************* added record: no address *******************'
#        
#    def csv_list(self):
#        items = [getattr(self, item) for item in self.csv_headers]
#        #items = [self.list_price, self.mls, self.address, self.region, self.year_built, self.beds, self.bath]
#        
#        return items
#        
#    def __str__(self):
#        
#        #print self.bs_table
#        data = ['address', 'mls', 'list_price', 'region', 'rent_total', 'school_dist', 'stories', 'status', 'year_built']
#        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])


def get_mls():

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
    
    for region in REGIONS:
        
        print '-------------- STARTING ', region

        # get the file name and path; read the file
        fname = 'purchase_{0}_{1}.txt'.format(PLEX, region)
        fpath = '../html_source/purchase_multiplex/{0}'.format(fname)
        f = open(fpath)
        text = f.read()
        
#        soup = BeautifulSoup(text)
#        
#        scripts = soup.findAll('script')
#        [script.extract() for script in scripts]
#        imgs = soup.findAll('img')
#        [img.extract() for img in imgs]
#        
#        
#        # Get the property tables from the html.  However, some are nested in outer property tables
#        # so make a list of tables that do not have property tables as children.
#        property_tables_super = soup.findAll('table', 'PropertyDetailGridTable')
#        property_tables = []
#        for table in property_tables_super:
#            subtable = table.find('table', 'PropertyDetailGridTable')
#            if not subtable:
#                property_tables.append(table)
        
        
        property_tables = get_property_tables_from_text(text)
        
        for table in property_tables:
            
            #print table.prettify()
            try:
                entry = MLXMultiPurchase(bs_table=table, plex=2, region=region)    
            except:
                print table.prettify()
                raise
            
            if not entry.mls in mls_numbers:
                #print(entry)
                mls_numbers.append(entry.mls)
                results.append(entry)


    return results

def save_records():
    
    print 'starting save records'
            
    
    entries = get_mls()
    
    for entry in entries:
        entry.save(test=False)
        
        
    print '************************ DONE ******************'




save_records()

#results = get_mls()
#for result in results:
#    print result
#    print

    
