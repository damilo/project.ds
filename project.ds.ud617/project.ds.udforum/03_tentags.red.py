#!/usr/bin/python

"""
Task:
Output top 10 tags, ordered by the number of questions they appear in.
Hints:
Code should not use a data structure (e.g. a dictionary) in the reducer that stores a large number of keys.
Remember that Hadoop already sorts the mapper output based on key.
"""

# REDUCER ----------

"""
input: key = tag word, value = node type (1 = question, 0 = everything else)
output: key = tag word, value = number of questions / sorted descending by value

[i] the sense behind numbering the node type:
The reducer has to add up the node type per tag word to get the number of questions it apperas in.
"""

import sys
import csv

def keyFunc (e):
    return e[1]

def reducer ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    k = 10
    tag_list = []
    old_tagword = None
    sum_questions = 0
    
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
        
        # retrieve data to be processed
        node_tagword = line[0]
        node_type = int (line[1])

        # process data
        
        # sum up node type, as long as there's no change in node tag word
        if ((old_tagword == None) or (node_tagword == old_tagword)):
            sum_questions += node_type
            old_tagword = node_tagword
        # if there's change in node tag word...
        else:
            # ...fill list with top k <tag word-sum node type> pairs
            if (len (tag_list) <= k):
                tag_list.append ([old_tagword, sum_questions])
            # if elements in list with top k pairs > k...
            else:
                # sort top k list ascending
                tag_list.sort (key = keyFunc)
                # run through top k list and replace existent element if necessary
                for i in range (k):
                    if (sum_questions > tag_list[i][1]):
                        tag_list[i] = [old_tagword, sum_questions]
                        break
            
            old_tagword = node_tagword
            sum_questions = node_type
    
    # add last key-value pair
    # ...fill list with top k <tag word-sum node type> pairs
    if (len (tag_list) <= k):
        tag_list.append ([old_tagword, sum_questions])
    # if elements in list with top k pairs > k...
    else:
        # sort top k list ascending
        tag_list.sort (key = keyFunc)
        # run through top k list and replace existent element if necessary
        for i in range (k):
            if (sum_questions > tag_list[i][1]):
                tag_list[i] = [old_tagword, sum_questions]
                break

    # sort top k list
    tag_list.sort (key = keyFunc)
    # order descending
    tag_list = tag_list[::-1]

    n = len (tag_list) if len (tag_list) < k else k
    # output key and value
    for i in range (n):
        writer.writerow (tag_list[i])


# UNIT TEST ----------

test_input = """\"12three\"\t\"1\"
\"230\"\t\"0\"
\"cs101\"\t\"0\"
\"hello\"\t\"1\"
\"hello\"\t\"1\"
\"miao\"\t\"0\"
\"panic\"\t\"1\"
\"que\"\t\"0\"
\"tag\"\t\"0\"
\"word\"\t\"0\"
\"you\"\t\"1\"
\"bam\"\t\"1\"
"""

from io import StringIO
def main ():

    sys.stdin = StringIO (test_input)
    reducer ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()