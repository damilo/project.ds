#!/usr/bin/python

"""
Task:
For each forum thread (that is a question node with all it's answers and comments),
give a list of students that have posted there - either asked the question, answered a question or added a comment.
If a student posted to that thread several times, they should be added to that list several times as well,
to indicate intensity of communication.
"""

# MAPPER ----------

"""
input: forum_node.tsv
output: key = forum (parent) thread id, value = node type (question, answer, comment) and user id
"""

import sys
import csv
import re

def mapper ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    header = True
    fields_per_line = 19
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
        author_id = line[3]
        node_type = line[5]
        parent_node_id = line[6]

        # process data
        key = None
        # filter posts and create unique keys -> this will make a nice sorting for the reducer
        if ((parent_node_id == '\\N') and (node_type == 'question')):
            key = '{0}.{1}'.format (node_id, -1)
        else:
            key = '{0}.{1}'.format (parent_node_id, node_id)

        # output key and value
        writer.writerow ([key, node_type, author_id])


# UNIT TEST ----------

test_input = """\"id\"\t\"title\"\t\"tagnames\"\t\"author_id\"\t\"body\"\t\"node_type\"\t\"parent_id\"\t\"abs_parent_id\"\t\"added_at\"\t\"score\"\t\"state_string\"\t\"last_edited_id\"\t\"last_activity_by_id\"\t\"last_activity_at\"\t\"active_revision_id\"\t\"extra\"\t\"extra_ref_id\"\t\"extra_count\"\t\"marked\"
\"id1\"\t\"-\"\t\"hello you\"\t\"001\"\t\"id1_body\"\t\"question\"\t\"\\N\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"only 18 fields\"\t\"-\"\t\"-\"\t\"001\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id2\"\t\"-\"\t\" tag word \"\t\"002\"\t\"id2_bodybody\"\t\"answer\"\t\"id1\"\t\"-\"\t\"2012-02-27 22:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id3\"\t\"-\"\t\" !=#$\"\t\"002\"\t\"id3_body3\"\t\"question\"\t\"\\N\"\t\"-\"\t\"2012-02-27 25:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"-\"\t\"-\"\t\"miao cs101 230\"\t\"003\"\t\"-\"\t\"comment\"\t\"-\"\t\"-\"\t\"2010-03-27 13:09:06.34523+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id2\"\t\"-\"\t\"?que? ?\"\t\"002\"\t\"move_body\"\t\"answer\"\t\"id1\"\t\"-\"\t\"2012-02-27 22:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id4\"\t\"-\"\t\" ? hello <panic> 12three\"\t\"001\"\t\"id1_body\"\t\"question\"\t\"\\N\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
"""

from io import StringIO
def main ():
    sys.stdin = StringIO (test_input)
    mapper ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()