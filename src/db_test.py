

import MySQLdb as mdb   #@UnresolvedImport

from datetime import datetime

from BeautifulSoup import BeautifulSoup


now = datetime.now()
datef = now.strftime("%Y-%m-%d")

def update_longitude():

#    nowf = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#    offer_count = 1000
#    low_price = 20.5
#    event_start = '2012-2-20'
    
    longitude = -99.123456789
    longitude = round(longitude,6)
    latitude = None
    region = None
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    #con.autocommit()
    cur = con.cursor()

    query = """ UPDATE austin_lease SET longitude=%s, latitude=%s, region=%s
        WHERE id=1 """
    cur.execute(query, (longitude, latitude, region ))
    
    cur.close()
    con.commit()
    con.close()
    
    print "************ done ****************"

#update_longitude()



def fetch_recs1():
    
    """
    Select *
    fetchmany(n)
    This returns tuples with all column values
    """
    
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor()
    query = """ SELECT * FROM austin_lease where longitude IS NULL """
    cur.execute(query)
    recs = cur.fetchmany(5)
    for rec in recs:
        print rec
    
    cur.close()
    con.close()
    
    print "************ done ****************"

#fetch_recs1()


def fetch_recs2():
    
    """
    Select *
    fetchmany(n)
    This returns tuples with one value
    """
    
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor()
    query = """ SELECT address FROM austin_lease where longitude IS NULL """
    cur.execute(query)
    recs = cur.fetchmany(5)
    for rec in recs:
        print rec
    
    cur.close()
    con.close()
    
    print "************ done ****************"

#fetch_recs2()

def fetch_recs3():
    
    """
    Using dictionary cursor
    This returns tuples with all column values
    """   
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ SELECT * FROM austin_lease where longitude IS NULL """
    cur.execute(query)
    recs = cur.fetchmany(5)
    for rec in recs:
        print rec
    
    cur.close()
    con.close()
    
    print "************ done ****************"

#fetch_recs3()

def fetch_recs4():
    
    """
    A query that returns no results
    This returns tuples with all column values
    """   
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ SELECT * FROM austin_lease where address = '###TIMBUKTU###' """
    cur.execute(query)
    recs = cur.fetchall()
    print recs
    
    cur.close()
    con.close()
    
    print "************ done ****************"

#fetch_recs4()


def insert1():
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """INSERT INTO austin_lease (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added)
      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
    data = [666, 'foo', 'foo', 'foo', 
            None, 666, 3, 2, 1000, datef]
    cur.execute(query, data)
    cur.close()
    con.commit()
    con.close()
    print "************ done ****************"

#insert1()

def insert2():
    
    """
    Check for the existence of a record before inserting
    """
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    
    query = """ SELECT COUNT(*) FROM austin_lease WHERE mls = %s
    """
    data = [666]
    cur.execute(query, data)
    recs = cur.fetchone()
    print recs
    if recs['COUNT(*)'] == 0:
        print 'inserting'
        query = """INSERT INTO austin_lease (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added)
          VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
        data = [666, 'foo', 'foo', 'foo', 
                None, 666, 3, 2, 1000, datef]
        cur.execute(query, data)
    cur.close()
    con.commit()
    con.close()
    print "************ done ****************"

#insert2()

def delete1():

    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)

    query = """ DELETE FROM austin_lease WHERE mls = 666    
    """    
    cur.execute(query)
    
    cur.close()
    con.commit()
    con.close()
    
    print "************ done ****************"

#delete1()


def count():
    

    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)

    query = """ SELECT list_price FROM austin_lease WHERE mls = 666    
    """    
    cur.execute(query)
    rec = cur.fetchall()
    print rec
    
#    print rec['COUNT(*)'] + 5
#    print rec['COUNT(*)']
    
    cur.close()
    con.close()
    
    print "************ done ****************"

count()    