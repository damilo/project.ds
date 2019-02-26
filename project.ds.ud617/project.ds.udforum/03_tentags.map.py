#!/usr/bin/python

"""
Task:
Output top 10 tags, ordered by the number of questions they appear in.
Hints:
Code should not use a data structure (e.g. a dictionary) in the reducer that stores a large number of keys.
Remember that Hadoop already sorts the mapper output based on key.
"""

# MAPPER ----------

"""
input: forum_node.tsv
output: key = tag word, value = node type (1 = question, 0 = everything else) // [i] in fact no value needed for this task

[i] the sense behind numbering the node type:
The reducer has to add up the node type per tag word to get the number of questions it apperas in.
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
        node_tagnames = line[2]
        node_type = 1 if (line[5] == 'question') else 0

        # process data
        # split tagnames into single words at any non-word characters
        re_non_word_chars = r'[\W]'
        node_tagwords = re.split (re_non_word_chars, node_tagnames)

        # output key and value
        for word in node_tagwords:
            if (len (word) > 0):
                writer.writerow ([word, node_type])


# UNIT TEST ----------

test_input = """\"id\"\t\"title\"\t\"tagnames\"\t\"author_id\"\t\"body\"\t\"node_type\"\t\"parent_id\"\t\"abs_parent_id\"\t\"added_at\"\t\"score\"\t\"state_string\"\t\"last_edited_id\"\t\"last_activity_by_id\"\t\"last_activity_at\"\t\"active_revision_id\"\t\"extra\"\t\"extra_ref_id\"\t\"extra_count\"\t\"marked\"
\"id1\"\t\"-\"\t\"hello you\"\t\"001\"\t\"id1_body\"\t\"question\"\t\"\\N\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"only 18 fields\"\t\"-\"\t\"-\"\t\"001\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id2\"\t\"-\"\t\" tag word \"\t\"002\"\t\"id2_bodybody\"\t\"answer\"\t\"id1\"\t\"-\"\t\"2012-02-27 22:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id3\"\t\"-\"\t\" !=#$\"\t\"002\"\t\"id3_body3\"\t\"question\"\t\"\\N\"\t\"-\"\t\"2012-02-27 25:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"-\"\t\"-\"\t\"miao cs101 230\"\t\"003\"\t\"-\"\t\"comment\"\t\"-\"\t\"-\"\t\"2010-03-27 13:09:06.34523+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id2\"\t\"-\"\t\"?que? ?\"\t\"002\"\t\"move_body\"\t\"answer\"\t\"id1\"\t\"-\"\t\"2012-02-27 22:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"id1\"\t\"-\"\t\" ? hello <panic> 12three\"\t\"001\"\t\"id1_body\"\t\"question\"\t\"\\N\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
"""

from io import StringIO
def main ():

    sys.stdin = StringIO (test_input)
    mapper ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()