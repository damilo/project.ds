"""
Task:
Calculate the mean of sales per weekday
"""

# ----------

# mapper
"""
input: purchases.txt
output: key = weekday, value = sales in format "weekday"\t"sales"
"""
#!/usr/bin/python
import sys
import csv
from datetime import datetime

def mapper ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)

    for line in reader:
        
        # jump over non-conformant lines
        if (len (line) != 6):
            continue
        
        date, time, store, item, cost, payment = line

        key = datetime.strptime (date, "%Y-%m-%d").weekday () # [i] weekday ranges from 0 = Monday to 6 = Sunday
        value = cost

        # [i] writer automatically adds delimter \t between list elements
        writer.writerow ([key, value])


# reducer
"""
input: key = weekday, value = sales in format "weekday"\t"sales"
output: key = weekday, value = mean sales per weekday in format "weekday"\t"mean sales"
"""
#!/usr/bin/python
import sys
import csv

def reducer ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    oldWeekday = None
    sum_sales = 0.0
    num_sales = 0
    for line in reader:
        
        # jump over non-conformant lines
        if (len (line) != 2):
            continue
        
        thisWeekday, thisCost = line
        
        if ((thisWeekday == oldWeekday) or (oldWeekday == None)):
            # calculate current mean value
            sum_sales += float (thisCost)
            num_sales += 1
            # store old weekday processed
            oldWeekday = thisWeekday
            continue
        
        # output mean sales of old weekday
        key = oldWeekday
        value = sum_sales / num_sales
        # [i] writer automatically adds delimter \t between list elements
        writer.writerow ([key, value])

        # prepare for calculations for new weekday
        sum_sales = float (thisCost)
        num_sales = 1
        oldWeekday = thisWeekday
    
    # output mean sales of last weekday processed
    key = oldWeekday
    value = sum_sales / num_sales
    # [i] writer automatically adds delimter \t between list elements
    writer.writerow ([key, value])



# ----------
from io import StringIO

test_text = """\"2012-01-01\"\t\"09:00\"\t\"San Diego\"\t\"Music\"\t\"66.08\"\t\"Cash\""""

test_text_red = """\"0\"\t\"66.08\"
\"0\"\t\"33.04\"
\"1\"\t\"12\"
\"1\"\t\"10\"
\"2\"\t\"8\"
\"2\"\t\"10\"
"""

def main ():
    sys.stdin = StringIO (test_text)
    mapper ()
    sys.stdin = StringIO (test_text_red)
    reducer ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()
