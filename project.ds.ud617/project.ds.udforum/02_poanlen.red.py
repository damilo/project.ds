#!/usr/bin/python

"""
Task:
Output the length of a post and the average answer length (just answer, not comment) for each post.
Hints:
Code should not use a data structure (e.g. a dictionary) in the reducer that stores a large number of keys.
Remember that Hadoop already sorts the mapper output based on key.
"""

# REDUCER ----------

"""
input: key = parent_node.node, value = node type and post length
output: key = post length, value = average answer length
"""

import sys
import csv

def reducer ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    old_node_id = None
    current_question_len = 0
    sum_answer_len = 0
    num_answers = 0
    
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
        
        # retrieve key and value
        node_id = line[0]
        node_type = line[1]
        node_body_len = line[2]

        # process data

        if (('.-1' in node_id) and (old_node_id != node_id) and (old_node_id != None)): # a change in question occured
            # [i] there were definitely answers of an old question before
            # value = length answer / count answers
            # key is length of question post
            average_answer_len = 0.
            if (num_answers != 0):
                average_answer_len = sum_answer_len / (num_answers*1.)
            writer.writerow ([old_node_id[:-3], current_question_len, average_answer_len])

        if ('.-1' in node_id):
            current_question_len = int (node_body_len)
            sum_answer_len = 0
            num_answers = 0
            old_node_id = node_id
        else:
            sum_answer_len += int (node_body_len)
            num_answers += 1

    # output of last line
    average_answer_len = 0.
    if (num_answers != 0):
        average_answer_len = sum_answer_len / (num_answers*1.)
    writer.writerow ([old_node_id[:-3], current_question_len, average_answer_len])


# UNIT TEST ----------

test_input = """\"id1.-1\"\t\"question\"\t\"8\"
\"id1.id2\"\t\"answer\"\t\"12\"
\"id1.id2\"\t\"answer\"\t\"9\"
\"id3.-1\"\t\"question\"\t\"9\"
\"id3.id4\"\t\"answer\"\t\"78\"
\"id3.id5\"\t\"answer\"\t\"62\"
\"id4.-1\"\t\"question\"\t\"31\"
\"id5.-1\"\t\"question\"\t\"12\"
\"id5.id6\"\t\"answer\"\t\"4\"
\"id5.id7\"\t\"answer\"\t\"8\"
"""

from io import StringIO
def main ():

    sys.stdin = StringIO (test_input)
    reducer ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()