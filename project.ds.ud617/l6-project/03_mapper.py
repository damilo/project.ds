"""
task:
Find the total sales value across all the stores,
and the total number of sales.
Assume there is only one reducer.

-> sales by store
key: store
value: sales
"""

#!/usr/bin/python

import sys

for line in sys.stdin:

    data = line.strip ().split ('\t')
    
    if (len (data) != 6):
        continue
    
    date, time, store, item, sales, payment = data

    key = store
    value = sales

    print (
        '{0}\t{1}'.format (key, value)
    )
