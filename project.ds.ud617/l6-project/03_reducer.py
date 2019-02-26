"""
task:
Find the total sales value across all the stores,
and the total number of sales.
Assume there is only one reducer.

-> total sales by store
-> number of sales by store
"""

"""
[i] data comes in sorted
"""

#!/usr/bin/python

import sys

oldKey = None
salesTotal = 0.0
numSalesPerStore = 0

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
            '{0}\t{1}\t{3}'.format (oldKey, salesTotal, numSalesPerStore)
        )
        salesTotal = 0.0 # reset for new product category
        numSalesPerStore = 0
    
    # otherwise just sum up the current sales,
    # for the current product category
    salesTotal += float (curSales)
    # and remember the product category of the recent processed line
    oldKey = curKey

    numSalesPerStore += 1

# if end of stdin stream is reached,
# output the sales for the current product category
print (
    '{0}\t{1}\t{2}'.format (curKey, salesTotal, numSalesPerStore)
)

"""
If we assume, that there's only one reducer,
we only need to continuously count the sales.

This version counts the number of sales per store.
In the end, we need another script to sum up all sales per store. (post processing)

"""