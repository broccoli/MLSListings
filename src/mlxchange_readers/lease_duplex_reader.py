
import csv

import MySQLdb as mdb   #@UnresolvedImport


from utils import MLXLease, get_property_tables_from_text


REGIONS = ['central', 'east', 'north_central', 'northwest', 'south', 'southeast', 'southwest', 'west']
#REGIONS = ['east']

def get_mlx(regions):

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
            
        print ' --------------- STARTING ', region
    
        # get the file name and path; read the file
        fname = 'lease_duplex_{0}.txt'.format(region)
        fpath = '../html_source/lease_duplex/{0}'.format(fname)
        f = open(fpath)
        text = f.read()
        
        property_tables = get_property_tables_from_text(text)

        
        
        for table in property_tables:
            
            #print table.prettify()
            try:
                entry = MLXLease(bs_table=table, region=region)
                    
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
            
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    
    entries = get_mlx(regions)
    
    for entry in entries:
        entry.save(con)
        
    con.commit()
    con.close()
        
    print '************************ DONE ******************'


def write_csv(list_type, source_file=None):
    
    entries = get_mlx(list_type)
    
    with open('output/purchase_condo.csv', 'wb') as csvfile:
        writer = csv.writer(csvfile)
        
        # write header row with csv_list from first entry
        writer.writerow(entries[0].csv_headers)
        
        for entry in entries:
            writer.writerow(entry.csv_list())
            
    print '************** DONE ***********'


save_records(REGIONS)

#entries = get_mlx(regions = REGIONS)
##
#for entry in entries:
#    print entry
#    print



#def get_mlx():
#
#    """
#    The function returns a list of MLSLease objects.
#    The function reads html documents and retrieves tables containing the data with Beautiful Soup.
#    The Beautiful Soup tables are passed to the constructors.
#    
#    The function will read all the html files for a given list of regions and a given type
#    of listing -- sale or lease.    
#    """
#
#    # keep list of mls numbers to check for duplicates.  I tried to get only the lowest
#    # level of nested table with Bueautiful Soup, but I'm getting occasional sets of 
#    # nested tables, that is, the data is embedded in two levels of tables so it's
#    # getting loaded twice.
#    mls_numbers = []
#    results = []
#    
#    
#    # get list of filenames
#    
#    
#    # get the file name and path; read the file
#    fname = 'lease_duplex_all.txt'
#    fpath = '../html_source/{0}'.format(fname)
#    f = open(fpath)
#    text = f.read()
#    
#    
#    property_tables = get_property_tables_from_text(text)
#
#    
#    for table in property_tables:
#        
#        #print table.prettify()
#        try:
#            entry = MLXLease(bs_table=table)
#                
#        except:
#            print table.prettify()
#            raise
#        
#        if not entry.mls in mls_numbers:
#            #print(entry)
#            mls_numbers.append(entry.mls)
#            results.append(entry)
#
#
#    return results
#
#def save_records():
#    
#    print 'starting save records'
#            
#    con = mdb.connect('localhost', 'root', 'password', 'submarine');
#    
#    entries = get_mlx()
#    
#    for entry in entries:
#        entry.save(con)
#        
#    con.commit()
#    con.close()
#        
#    print '************************ DONE ******************'
#
#
#def write_csv(list_type, source_file=None):
#    
#    entries = get_mlx(list_type)
#    
#    with open('output/purchase_condo.csv', 'wb') as csvfile:
#        writer = csv.writer(csvfile)
#        
#        # write header row with csv_list from first entry
#        writer.writerow(entries[0].csv_headers)
#        
#        for entry in entries:
#            writer.writerow(entry.csv_list())
#            
#    print '************** DONE ***********'
#
#
##save_records()
#
##write_csv(LIST_TYPE)
#
#
#
##save_records(list_type = LIST_TYPE)
#
#entries = get_mlx()
##
#for entry in entries:
#    print entry
#    print









