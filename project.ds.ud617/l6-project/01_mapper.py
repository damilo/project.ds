"""
task:
Instead of breaking the sales down by store,
instead give us a sales breakdown by product category across all of our stores.

-> sales breakdown by product category across all stores
key: product category
value: sales
"""

#!/usr/bin/python

import sys

for line in sys.stdin:

    data = line.strip ().split ('\t')
    
    if (len (data) != 6 ):
        continue
    
    date, time, store, item, cost, payment = data

    key = item
    value = cost

    print (
        '{0}\t{1}'.format (key, value)
    )
