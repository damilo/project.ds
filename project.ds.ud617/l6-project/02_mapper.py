"""
task:
Find the monetary value for the highest individual sale for each separate store.

-> instance with highest cost by store
key: store
value: cost
"""

#!/usr/bin/python

import sys

for line in sys.stdin:

    data = line.strip ().split ('\t')
    
    if (len (data) != 6 ):
        continue
    
    date, time, store, item, cost, payment = data

    key = store
    value = cost

    print (
        '{0}\t{1}'.format (key, value)
    )
