


import MySQLdb as mdb   #@UnresolvedImport

def mark_as_exclude():

    print 'starting'

    f = open('../text_source/mls_to_exclude_from_sale.txt')    
    lines = f.read().split('\n')

    mls_list = []    
    for line in lines:
        
        try:
            mls = int(line)
            mls_list.append(mls)
        except:
            pass

    for mls in mls_list:
        
        con = mdb.connect('localhost', 'root', 'password', 'submarine');
        cur = con.cursor(mdb.cursors.DictCursor)
        query = """ update austin_sale set exclude = 1
            WHERE mls=%s """
        data = [mls]
        cur.execute(query, data)
        cur.close()
        con.commit()
        con.close()

    print 'done'
    
mark_as_exclude()