

from utils.utils import get_google_data

from time import sleep


import MySQLdb as mdb   #@UnresolvedImport




def update_geocode_for_pct_rent():
    
    print 'starting'

    # query all sale records >= 1.0 pct rent
    
    
    # get average rent for number of bedrooms for each zip code
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ select mls, address, id
    from austin_sale where pct_rent >= 0.9 and geocode_status is null and exclude = 0 """
    cur.execute(query)
    recs = cur.fetchall()
    
    #print len(recs)
    
    cur.close()
    con.close()
    
    print '--------- Geocoding {0} records; beginning in 3 seconds -------'.format(len(recs))
    
    sleep(3)

    # loop through mls, get google data, update sale record
    
    for rec in recs:
        
        if rec['address']:
            google_data = get_google_data(rec['address'])
            
            con = mdb.connect('localhost', 'root', 'password', 'submarine');
            cur = con.cursor(mdb.cursors.DictCursor)
            
            query = """ UPDATE austin_sale SET latitude=%s, longitude=%s, neighborhood=%s, geocode_status=%s
                WHERE id=%s """
            print query
            data = []
            data.append(google_data.latitude)
            data.append(google_data.longitude)
            data.append(google_data.neighborhood)
            data.append(google_data.google_status)
            data.append(rec['id'])
            cur.execute(query, data)
            
            cur.close()
            con.commit()
            con.close()
        
    print '************* done ***************'
    
    

update_geocode_for_pct_rent()
