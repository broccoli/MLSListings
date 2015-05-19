

from utils.utils import get_google_data

import MySQLdb as mdb   #@UnresolvedImport


def geocode_lease():
    
    print 'starting'

    # query all sale records >= 1.0 pct rent
    
    
    # get average rent for number of bedrooms for each zip code
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ select address, id
    from austin_lease where geocode_status is null
    """
    cur.execute(query)
    recs = cur.fetchall()
    
    cur.close()
    con.close()
    
    print ' number of recs to update: ', len(recs)

    # loop through mls, get google data, update sale record
    
    ctr = 1
    
    for rec in recs:
        
        print 'Geocoding {0}'.format(ctr)
        
        ctr += 1
        
        if rec['address']:
            google_data = get_google_data(','.join([rec['address'], 'Austin', 'TX']))
            
            con = mdb.connect('localhost', 'root', 'password', 'submarine');
            cur = con.cursor(mdb.cursors.DictCursor)
            
            query = """ UPDATE austin_lease SET latitude=%s, longitude=%s, neighborhood=%s, zip=%s, geocode_status=%s
                WHERE id=%s """
            #print query
            data = []
            data.append(google_data.latitude)
            data.append(google_data.longitude)
            data.append(google_data.neighborhood)
            data.append(google_data.zip)
            data.append(google_data.google_status)
            data.append(rec['id'])
            
            try:
                cur.execute(query, data)
            except:
                print "************************** BAD GEOCODE DATA"
                print google_data
                print
            
            cur.close()
            con.commit()
            con.close()
        
    print '************* done ***************'
    
    

geocode_lease()
