#!/usr/bin/python

"""
Task:
For each student, find the hour during which the student has been posted the most posts.
reducer ouptut: key = author_id, value = hour
If there are multiple hours where a student has posted the most posts, output each hour in a separate line.
Ignore timezone offset for all times.
Use field 'added_at' to retrieve the hour posted.
"""

"""
input: key = author_id, value = added_at hour
outpu: key = author_id, value = hour with most posts
"""

import sys
import csv
from datetime import datetime
from collections import defaultdict

def reducer ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    oldKey = None
    oldValue = None
    sum_posts_per_thisHour = 0
    sum_posts_per_oldHour = 0
    dict_posts_per_hour = defaultdict (lambda: 0)
    
    header = False
    fields_per_line = 2
    for line in reader:

        # jump over header
        if (header):
            header = False
            continue

        # check if line has the correct number of fields
        if (len (line) != fields_per_line):
            continue

        # retrieve key and value
        thisKey = line[0] # 'author_id'
        thisValue = int (line[1]) # 'added_at hour'


        if (oldKey and oldKey != thisKey):
            # sort descending
            sorted_d = sorted ((num_posts, hour) for (hour, num_posts) in dict_posts_per_hour.items())
            sorted_d = sorted_d[::-1]
            # output key and value
            tmpMostPosts = 0
            for num_posts, hour in sorted_d:
                if (num_posts >= tmpMostPosts):
                    writer.writerow ([oldKey, hour])
                    tmpMostPosts = num_posts
                else:
                    break
            
            # reset temp vars
            dict_posts_per_hour = defaultdict (lambda: 0)
        
        
        # per author_id, count posts per hour
        if (oldValue == thisValue):
            dict_posts_per_hour[oldValue] += 1
        else:
            dict_posts_per_hour[thisValue] += 1
        
        # store old values
        oldValue = thisValue
        oldKey = thisKey

    # output key and value for last author/hour
    # sort descending
    sorted_d = sorted ((num_posts, hour) for (hour, num_posts) in dict_posts_per_hour.items())
    sorted_d = sorted_d[::-1]
    # output key and value
    tmpMostPosts = 0
    for num_posts, hour in sorted_d:
        if (num_posts >= tmpMostPosts):
            writer.writerow ([oldKey, hour])
            tmpMostPosts = num_posts
        else:
            break
    


# UNIT TEST ----------

test_input = """\"001\"\t"8"
\"001\"\t"8"
\"001\"\t"12"
\"002\"\t\"22\"
\"002\"\t\"22\"
\"002\"\t\"22\"
\"002\"\t\"23\"
\"002\"\t\"23\"
\"003\"\t\"13\"
\"004\"\t\"13\"
\"004\"\t\"5\"
\"004\"\t\"5\"
\"004\"\t\"5\"
\"004\"\t\"7\"
\"004\"\t\"7\"
\"005\"\t\"5\"
\"005\"\t\"5\"
\"005\"\t\"7\"
\"005\"\t\"7\"
\"006\"\t\"5\"
\"006\"\t\"7\"
\"006\"\t\"7\"
\"006\"\t\"9\"
\"006\"\t\"9\"
\"006\"\t\"9\"
\"006\"\t\"10\"
\"006\"\t\"10\"
\"006\"\t\"10\"
"""

from io import StringIO
def main ():

    print ('output:')
    sys.stdin = StringIO (test_input)
    reducer ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()