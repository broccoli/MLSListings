

class Purchase():

    def __init__(self, color, food, animal):
        self.color = color
        self.food = food
        self.animal = animal

p1 = Purchase('blue', 'pizza', 'dog')

fields = ['color', 'food', 'animal']
table = 'austin_sale'


s_places = ', '.join(['%s' for index in range(len(fields))])


query = "insert into {0} ({1}) VALUES ({2})".format(table, ', '.join(fields), s_places)

data = [getattr(p1, field) for field in fields]
print query
print data


#query = """INSERT INTO austin_sale (mls, property_type, region, address, unit, list_price, beds, bath, sf, date_added,
#year_built, school_dist, zip)
#VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""" 
