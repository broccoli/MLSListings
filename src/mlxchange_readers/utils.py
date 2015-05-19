
import re
from datetime import datetime
from BeautifulSoup import BeautifulSoup
import MySQLdb as mdb   #@UnresolvedImport

now = datetime.now()
datef = now.strftime("%Y-%m-%d")

class MLXLease():
    
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
    
    def __init__(self, bs_table, region=None):
        self.region = region
        self.property_type = bs_table.findNext(text=re.compile('Type')).parent.findNext('b').string
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
        
            query = """INSERT INTO austin_lease (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added, current)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
            data = [self.mls, self.property_type, self.region, self.address, 
                    self.unit, self.list_price, self.beds, self.bath, self.sf, datef, 1]
            cur.execute(query, data)
            cur.close()
            if self.address:
                print '**************** added record: ' + self.address + ' ******************'
            else:
                print '******************* added record: no address *******************'

        else:
            print 'marking duplicate: ', self.mls
            query = """ UPDATE austin_lease SET current=1
                WHERE mls=%s """
            cur.execute(query, (self.mls,))
            cur.close()

        
    def __str__(self):
        
        #print self.bs_table
        data = ['address', 'property_type', 'region', 'mls', 'unit', 'list_price', 'city', 'beds', 'bath', 'sf']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])



class MLXPurchase():
    
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

    def save(self, test=True):
        
        """
        When saving, see if the mls number is on the table.  If so, see if the list price
        has changed and update.
        
        when test == True, no commits
        """
        # get record if it exists
        con = mdb.connect('localhost', 'root', 'password', 'submarine');
        cur = con.cursor(mdb.cursors.DictCursor)
        query = """ SELECT list_price FROM austin_sale WHERE mls = %s """
        data = [self.mls]
        cur.execute(query, data)
        recs = cur.fetchall()
        cur.close()
        con.close()

        if recs:
            if len(recs) > 1:
                raise Exception(''.join(['more than one record for', self.mls]))
            # check for new list price
            if self.list_price != recs[0]['list_price']:
                print '******* NEW LIST PRICE FOR {0} *********'.format(self.mls)
                print '{0} changed to {1}'.format(recs[0]['list_price'], self.list_price)
                
                con = mdb.connect('localhost', 'root', 'password', 'submarine');
                cur = con.cursor()
                query = """ update austin_sale set list_price=%s WHERE mls = %s """
                cur.execute(query, (self.list_price, self.mls))
                cur.close()
                if test == False:
                    con.commit()
                con.close()
            else:
                print 'skipping duplicate: ', self.mls
                
        else:
            con = mdb.connect('localhost', 'root', 'password', 'submarine');
            cur = con.cursor()
            query = """INSERT INTO austin_sale (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added,
                year_built, school_dist, zip)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
            data = [self.mls, self.property_type, self.region, self.address, 
                    self.unit, self.list_price, self.beds, self.bath, self.sf, datef, self.year_built, self.school_dist, self.zip]
            cur.execute(query, data)
            cur.close()
            if test == False:
                con.commit()
            con.close()
            if self.address:
                print '**************** added record: {0}: {1} ******************'.format(self.mls, self.address)
            else:
                print '******************* added record: no address *******************'

            
        
    def csv_list(self):
        items = [getattr(self, item) for item in self.csv_headers]
        #items = [self.list_price, self.mls, self.address, self.region, self.year_built, self.beds, self.bath]
        
        return items
        
    def __str__(self):
        
        #print self.bs_table
        data = ['address', 'mls', 'list_price', 'region', 'beds', 'bath', 'sf', 'year_built', 'school_dist', 'zip']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])


class MLXMultiPurchase():
    
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
    
    def __init__(self, bs_table, plex, region):
        self.bs_table = bs_table
        self.plex = plex    # 2, 3, 4
        self.region = region
        
        self.mls = bs_table.findNext(text=re.compile('MLS')).parent.findNext('b').string
        self.address = bs_table.findNext(text=re.compile('Address')).parent.findNext('b').string
        if self.address:
            self.address = re.sub(' +', ' ', self.address)
        self.list_price_str = bs_table.findNext(text=re.compile('List')).parent.findNext('b').string
        self.list_price = int(self.list_price_str.replace(',', ''))
        self.rent_total_str = bs_table.findNext(text=re.compile('RentTotal')).parent.findNext('b').string
        self.rent_total = int(self.rent_total_str)
        self.school_dist = bs_table.findNext(text=re.compile('Schl')).parent.findNext('b').string        
        self.stories_str = bs_table.findNext(text=re.compile('Stories')).parent.findNext('b').string
        self.stories = int(self.stories_str)
        self.status = bs_table.findNext(text=re.compile('Status')).parent.findNext('b').string        
        self.year_built_str = bs_table.findNext(text=re.compile('Yr')).parent.findNext('b').string
        self.year_built = int(self.year_built_str)
        
        self.csv_headers = ['list_price', 'mls', 'address', 'region', 'year_built']

    def save(self, test=True):
        
        # get record if it exists
        con = mdb.connect('localhost', 'root', 'password', 'submarine');
        cur = con.cursor(mdb.cursors.DictCursor)
        query = """ SELECT list_price, status FROM multiplex_sale WHERE mls = %s    
        """
        data = [self.mls]
        cur.execute(query, data)
        recs = cur.fetchall()
        cur.close()
        con.close()
        
        if recs:
            if len(recs) > 1:
                raise Exception(''.join(['more than one record for', self.mls]))
            if self.list_price != recs[0]['list_price']:
                print '******* NEW LIST PRICE FOR {0} *********'.format(self.mls)
                print '{0} changed to {1}'.format(recs[0]['list_price'], self.list_price)
            if self.status != recs[0]['status']:
                print '---------- NEW STATUS FOR {0} ---------'.format(self.mls)
                print '{0} changed to {1}'.format(recs[0]['status'], self.status)
            # if status or list price has changed, update both
            if self.status != recs[0]['status'] or self.list_price != recs[0]['list_price']:
                con = mdb.connect('localhost', 'root', 'password', 'submarine');
                cur = con.cursor()
                query = """ update multiplex_sale set status=%s, list_price=%s WHERE mls = %s """
                cur.execute(query, (self.status, self.list_price, self.mls))
                cur.close()
                if test == False:
                    con.commit()
                con.close()
                
        else:
            con = mdb.connect('localhost', 'root', 'password', 'submarine');
            cur = con.cursor()
            query = """INSERT INTO multiplex_sale (mls, plex, address, region, list_price, rent_total, school_district, stories, status, year_built, date_added)
              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
            data = [self.mls, self.plex, self.address, self.region, 
                    self.list_price, self.rent_total, self.school_dist, self.stories, self.status, self.year_built, datef]
            cur.execute(query, data)
            cur.close()
            con.commit()
            con.close()
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
        data = ['address', 'mls', 'list_price', 'region', 'rent_total', 'school_dist', 'stories', 'status', 'year_built']
        return '\n'.join(['{0}: {1}'.format(item, getattr(self, item)) for item in data])



def get_property_tables_from_text(text):
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

    return property_tables
