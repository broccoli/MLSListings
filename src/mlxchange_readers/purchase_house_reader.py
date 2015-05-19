



#import csv

#import MySQLdb as mdb   #@UnresolvedImport

from datetime import datetime

#from BeautifulSoup import BeautifulSoup

from utils import MLXPurchase, get_property_tables_from_text


now = datetime.now()
datef = now.strftime("%Y-%m-%d")

#REGIONS = ['central']
REGIONS = ['central', 'east', 'north_central', 'northwest', 'south', 'southeast', 'southwest', 'west']

#file_name = '{0}_{1}_{2}.txt'.format(LIST_TYPE, PROPERTY_TYPE, REGION)








def get_mlx(regions):

    """
    The function returns a list of MLSPurchase objects.
    The function reads html documents and retrieves tables containing the data with Beautiful Soup.
    The Beautiful Soup tables are passed to the constructors.
    
    The function will read all the html files for a given list of regions.    
    """

    # keep list of mls numbers to check for duplicates.  I tried to get only the lowest
    # level of nested table with Bueautiful Soup, but I'm getting occasional sets of 
    # nested tables, that is, the data is embedded in two levels of tables so it's
    # getting loaded twice.
    mls_numbers = []
    results = []
    
    
    # get list of filenames
    
    for region in regions:
            
        print '--------------- STARTING', region
    
        # get the file name and path; read the file
        fname = 'purchase_house_{0}.txt'.format(region)
        fpath = '../html_source/purchase_house/{0}'.format(fname)
        f = open(fpath)
        text = f.read()
        
        property_tables = get_property_tables_from_text(text)

        
        
        for table in property_tables:
            
            #print table.prettify()
            try:
                entry = MLXPurchase(bs_table=table, region=region, property_type='house')
                    
            except:
                print table.prettify()
                raise
            
            if not entry.mls in mls_numbers:
                #print(entry)
                mls_numbers.append(entry.mls)
                results.append(entry)


    return results

def save_records(regions):
    
    print 'starting save records'
            
    #con = mdb.connect('localhost', 'root', 'password', 'submarine');
    
    entries = get_mlx(regions)
    
    for entry in entries:
        entry.save(test=False)
        
    #con.commit()
    #con.close()
        
    print '************************ DONE ******************'




save_records(REGIONS)


#write_csv(LIST_TYPE)



#save_records(list_type = LIST_TYPE)

#entries = get_mls(LIST_TYPE)
#
#for entry in entries:
#    print entry








