
import json
import MySQLdb as mdb   #@UnresolvedImport
import decimal


class DecimalEncoder(json.JSONEncoder):
    def _iterencode(self, o, markers=None):
        if isinstance(o, decimal.Decimal):
            # wanted a simple yield str(o) in the next line,
            # but that would mean a yield on the line with super(...),
            # which wouldn't work (see my comment below), so...
            return (str(o) for o in [o])
        return super(DecimalEncoder, self)._iterencode(o, markers)

def get_sorted_list_of_attr_dicts(recs, attr_name):

    attrs = {}
    attrs_sort = []
    for rec in recs:
    
    
        # get dictionary of zip codes and their counts
        if rec[attr_name] in attrs.keys():
            attrs[rec[attr_name]] += 1;
        else:
            attrs[rec[attr_name]] = 1;
            attrs_sort.append(rec[attr_name])
    
    attrs_sort.sort()
    
    attrs_out = []
    for attr in attrs_sort:
        attrs_out.append({attr_name: attr, 'number':attrs[attr]})

    return attrs_out
 
def lease_output():
    
    # get all records
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ SELECT list_price, latitude, longitude, mls, property_type, region, address,
        beds, bath, sf, zip
        FROM austin_lease WHERE geocode_status = 'OK' """
    cur.execute(query)
    recs = cur.fetchall()
    cur.close()
    con.close()

    f = open('C:\Users\Ric\Desktop\maps\lease_data.js', 'w')
#    f.write('qwert')
    f.write('var pRecords = [\n')
    
    high_rent = None
    low_rent = None
    for rec in recs:
        if rec == recs[-1]:
            f.write(json.dumps(rec, cls=DecimalEncoder) + '\n')
        else:
            f.write(json.dumps(rec, cls=DecimalEncoder) + ',\n')
            
        if high_rent is None or rec['list_price'] > high_rent:
            high_rent = rec['list_price']
        if low_rent is None or rec['list_price'] < low_rent:
            low_rent = rec['list_price']
#    
#    
#        # get dictionary of zip codes and their counts
#        if rec['zip'] in zips.keys():
#            zips[rec['zip']] += 1;
#        else:
#            zips[rec['zip']] = 1;
#            zips_sort.append(rec['zip'])
    f.write('];\n')
#    
#    zips_sort.sort()
#    
#    zip_out = []
#    for zip in zips_sort:
#        zip_out.append({'zip': zip, 'number':zips[zip]})

    zip_out = get_sorted_list_of_attr_dicts(recs, 'zip')
    f.write('var pZips = [\n')
    print zip_out
    for zipp in zip_out:
        if zipp == zip_out[-1]:
            f.write(json.dumps(zipp) + '\n')
        else:
            f.write(json.dumps(zipp) + ',\n')
    f.write('];\n')
    
    beds_out = get_sorted_list_of_attr_dicts(recs, 'beds')
    print beds_out
    f.write('var pBeds = [\n')
    for beds in beds_out:
        if beds == beds_out[-1]:
            f.write(json.dumps(beds) + '\n')
        else:
            f.write(json.dumps(beds) + ',\n')
    f.write('];\n')
    
    f.write('var pHigh_rent = {0};\n'.format(high_rent))
    f.write('var pLow_rent = {0};\n'.format(low_rent))

    f.close()

#    print zips
#    print zips_sort
    print zip_out


lease_output()


def sale_output():

    # get list of all sale records with geocode_status OK
    con = mdb.connect('localhost', 'root', 'password', 'submarine');
    cur = con.cursor(mdb.cursors.DictCursor)
    query = """ SELECT list_price, latitude, longitude, mls, region, address,
        beds, bath, sf, zip, pct_rent, ave_rent
        FROM austin_sale WHERE geocode_status = 'OK' AND exclude = 0 
        AND pct_rent >= 1 """
    cur.execute(query)
    recs = cur.fetchall()
    cur.close()
    con.close()
    
    # write record fields to js file
    f = open('C:\Users\Ric\Desktop\maps\sale_data.js', 'w')
    f.write('var pSales = [\n')
    for rec in recs:
        if rec == recs[-1]:
            f.write(json.dumps(rec, cls=DecimalEncoder) + '\n')
        else:
            f.write(json.dumps(rec, cls=DecimalEncoder) + ',\n')
    f.write('];\n')

#sale_output()

print 'done'
