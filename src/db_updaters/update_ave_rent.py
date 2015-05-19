

import MySQLdb as mdb   #@UnresolvedImport

beds_list = [2, 3, 4]

def update_ave_rent_for_houses(num):

    print 'starting'
    
    
    # get average rent for number of bedrooms for each zip code
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ select zip, round(avg(list_price)) as "ave"
    from austin_lease where beds = %s and property_type=%s and zip is not null
    group by zip """
    cur.execute(query, [num, 'HOUSE'])
    aves = cur.fetchall()
    
    cur.close()
    con.close()

      

    # loop through results
    for ave in aves:
        #print ave['ave']
        con = mdb.connect('localhost', 'root', 'password', 'submarine');
        cur = con.cursor(mdb.cursors.DictCursor)
        query = """ UPDATE austin_sale SET ave_rent=%s
            WHERE beds=%s and zip=%s """
        data = [ave['ave'], num, ave['zip']]
        cur.execute(query, data)
        cur.close()
        con.commit()
        con.close()
    
    # update records with the zip code and beds with the ave rent
    
    print 'done'


def update_ave_rent():
    for beds in beds_list:
        
        print '----------- starting ', beds
        update_ave_rent_for_houses(beds)

update_ave_rent()