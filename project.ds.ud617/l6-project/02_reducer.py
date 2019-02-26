"""
task:
Find the monetary value for the highest individual sale for each separate store.

-> instance with highest cost by store
key: store
value: cost
"""

"""
[i] data comes in sorted
"""

#!/usr/bin/python

import sys

oldKey = None
highestSales = 0.0

for line in sys.stdin:
    
    data = line.strip ().split ('\t')
    
    if (len (data) != 2):
        continue
    
    curKey, curSales = data
    
    # data is sorted,
    # if a change in store occurs,
    # then the highest sales value has already been found
    if oldKey and oldKey != curKey:
        print (
            '{0}\t{1}'.format (oldKey, highestSales)
        )
        highestSales = 0.0 # reset for new store
    
    # otherwise check if current sales value is higher than saved sales value,
    # for the current store
    if (float (curSales) > highestSales):
        highestSales = float (curSales)
    
    # and remember the store of the recent processed line
    oldKey = curKey

# if end of stdin stream is reached,
# output the highest sales for the current store
print (
    '{0}\t{1}'.format (curKey, highestSales)
)
