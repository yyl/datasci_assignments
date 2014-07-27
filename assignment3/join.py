import MapReduce
import sys

"""
Problem two
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[1]
    value = record
    mr.emit_intermediate(key, value)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = []
    orders = (value for value in list_of_values if value[0] == 'order')
    items = (value for value in list_of_values if value[0] == 'line_item')
    for order in orders:
        for item in items:
            mr.emit(order + item)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
