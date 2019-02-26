"""
task:
Find the total sales value across all the stores,
and the total number of sales.
Assume there is only one reducer.

-> number of sales by store
key: store
value: number of sales per store
"""

"""
This mapper functions as a post processor.
"""

"""
[i] data now comes in sorted, with unique keys
"""

import pandas as pd

all_stores_df = pd.read_csv ('./03_all_stores.txt', sep = '\t', names = ['store', 'total_sales', 'total_num_sales'])

print (all_stores_df.sum (axis = 0))