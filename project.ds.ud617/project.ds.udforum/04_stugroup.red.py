#!/usr/bin/python

"""
Task:
For each forum thread (that is a question node with all it's answers and comments),
give a list of students that have posted there - either asked the question, answered a question or added a comment.
If a student posted to that thread several times, they should be added to that list several times as well,
to indicate intensity of communication.
"""

# REDUCER ----------

"""
input: key = forum (parent) thread id, value = node type (question, answer, comment) and user id
output: key = forum (parent) thread id, value = list of users posted in the forum thread
"""

import sys
import csv

def keyFunc (e):
    return e[1]

def reducer ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    user_list = []
    old_parent_node_id = None
    
    header = False
    fields_per_line = 3
    for line in reader:

        # jump over header
        if (header):
            header = False
            continue

        # check if line has the correct number of fields
        if (len (line) != fields_per_line):
            continue
        
        # retrieve data to be processed
        node_id = line[0]
        node_type = line[1]
        user_id = line[2]

        # process data
        parent_node_id, _ = node_id.split ('.', 1)

        # new thread is being processed
        if ((old_parent_node_id != None) and (parent_node_id != old_parent_node_id)):
            # output old key-value pair
            writer.writerow ([old_parent_node_id, ', '.join (user_list)])
            user_list = []
        
        user_list.append (user_id)
        old_parent_node_id = parent_node_id
    
    # output last key-value pair
    writer.writerow ([old_parent_node_id, ', '.join (user_list)])


# UNIT TEST ----------

test_input = """\"-.-\"\t\"comment\"\t\"003\"
\"id1.-1\"\t\"question\"\t\"001\"
\"id1.id2\"\t\"answer\"\t\"002\"
\"id1.id2\"\t\"answer\"\t\"002\"
\"id3.-1\"\t\"question\"\t\"002\"
\"id4.-1\"\t\"question\"\t\"001\"
"""

from io import StringIO
def main ():

    sys.stdin = StringIO (test_input)
    reducer ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()