

"""
This program will delete records from austin_sale that
are not in the weekly text update

"""

from utils import MLXPurchase, get_property_tables_from_text

import MySQLdb as mdb   #@UnresolvedImport

REGIONS = ['central', 'east', 'north_central', 'northwest', 'south', 'southeast', 'southwest', 'west']
#REGIONS = ['east']



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

def get_old_mls_in_table():
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ SELECT mls FROM austin_sale """
    cur.execute(query)
    recs = cur.fetchall()
    cur.close()
    con.close()
    
    old_mls = []
    for rec in recs:
        old_mls.append(rec['mls'])
    return old_mls
    
def get_new_mls():
    new_entries = get_mlx(REGIONS)
    new_mls = []
    for entry in new_entries:
        new_mls.append(int(entry.mls))
    return new_mls

def get_mls_to_purge():

    """
    get a list of mls numbers to purge from austin_sale
    """
    # get a list of mls numbers in the updated text files
    
    old_mls = get_old_mls_in_table()
    new_mls = get_new_mls()

    mls_to_purge = []    
    for mls in old_mls:
        if mls not in new_mls:
            mls_to_purge.append(mls)
    
    return mls_to_purge

def delete_mls_from_table(mls):
    print '**** purging {0} *****'.format(mls)
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ DELETE FROM austin_sale WHERE mls = %s
    """
    cur.execute(query, [mls])
    cur.close()
    con.commit()
    con.close()
    
def purge_house():


    mls_to_purge = get_mls_to_purge()
    for mls in mls_to_purge:
        delete_mls_from_table(mls)

    print '*************************** done with purge ****************************'

purge_house()


#new_mls = get_new_mls()
#print new_mls