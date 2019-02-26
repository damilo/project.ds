"""
task:
Instead of breaking the sales down by store,
instead give us a sales breakdown by product category across all of our stores.

-> sales breakdown by product category across all stores
key: product category
value: sales
"""

"""
[i] data comes in sorted
"""

#!/usr/bin/python

import sys

oldKey = None
salesTotal = 0.0

for line in sys.stdin:
    
    data = line.strip ().split ('\t')
    
    if (len (data) != 2):
        continue
    
    curKey, curSales = data
    
    # data is sorted,
    # if a change in product category occurs,
    # then the calculation of the total sales is done
    if oldKey and oldKey != curKey:
        print (
            '{0}\t{1}'.format (oldKey, salesTotal)
        )
        salesTotal = 0 # reset for new product category
    
    # otherwise just sum up the current sales,
    # for the current product category
    salesTotal += float (curSales)
    # and remember the product category of the recent processed line
    oldKey = curKey

# if end of stdin stream is reached,
# output the sales for the current product category
print (
    '{0}\t{1}'.format (curKey, salesTotal)
)
