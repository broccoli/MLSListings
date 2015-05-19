
# import float division
from __future__ import division


import MySQLdb as mdb   #@UnresolvedImport


def update_pct_rent():

    print 'starting'
    
    
    # get list of all records with mls and ave_rent
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ select mls, ave_rent, list_price
    from austin_sale where ave_rent is not null """
    cur.execute(query)
    recs = cur.fetchall()
    
    cur.close()
    con.close()

    # loop through records and update pct_rent
    for rec in recs:
        
        # figure pct_rent
        #print 'ave_rent', rec['ave_rent']
        #print 'list_price', rec['list_price']
        pct_rent = round(rec['ave_rent'] / rec['list_price'] * 100, 2)

        #print pct_rent
        
        # update sale record
        
        con = mdb.connect('localhost', 'root', 'password', 'submarine');
        cur = con.cursor(mdb.cursors.DictCursor)
        query = """ UPDATE austin_sale SET pct_rent=%s
            WHERE mls=%s """
        data = [pct_rent, rec['mls']]
        cur.execute(query, data)
        cur.close()
        con.commit()
        con.close()
    
    # update records with the zip code and beds with the ave rent
    
    print 'done'
    
update_pct_rent()
