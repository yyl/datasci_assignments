import MapReduce
import sys

"""
Problem six
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

index = 5

def mapper(record):
    if record[0] == 'a':
        for i in xrange(index):
            mr.emit_intermediate((record[1], i), record)
    if record[0] == 'b':
        for i in xrange(index):
            mr.emit_intermediate((i, record[2]), record)

def reducer(key, list_of_values):
    i = key[0]
    j = key[1]
    a = {}
    b = {}
    for record in list_of_values:
        if record[0] == 'a':
            a[(record[1], record[2])] = record[3]
        if record[0] == 'b':
            b[(record[1], record[2])] = record[3]
    total = 0
    for k in xrange(index):
        total += a.get((i, k), 0) * b.get((k, j), 0)
    mr.emit((i, j, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
