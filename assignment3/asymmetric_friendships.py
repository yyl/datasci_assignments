import MapReduce
import sys

"""
Problem four
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    tup = sorted(record)
    mr.emit_intermediate(','.join(tup), 1)

def reducer(key, list_of_values):
    total = 0
    for v in list_of_values:
        total += 1
    if total == 1:
        a, b = key.split(',')
        mr.emit((a,b))
        mr.emit((b,a))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
