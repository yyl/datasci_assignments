import MapReduce
import sys

"""
Problem 1
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    doc_id = record[0]
    text = record[1]
    words = set(text.split())
    for w in words:
      mr.emit_intermediate(w, doc_id)

def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    total = []
    for v in list_of_values:
      total.append(v)
    mr.emit((key, total))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
