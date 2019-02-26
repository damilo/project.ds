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
input: forum_node.tsv
output: key = author_id, value = added_at hour
"""

import sys
import csv
from datetime import datetime

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

        # retrieve key and value
        key = line[3] # 'author_id'
        value = line[8] # 'added_at'

        # process data
        # format of field 'added_at' = YYYY-MM-DD HH:MM:SS.uS+TZ
        # remove time zone
        value = value[:-3]
        # you only need the hour HH
        # %Y-%m-%d %H:%M:%S.%f
        try:
            value = datetime.strptime (value, '%Y-%m-%d %H:%M:%S.%f').hour
        except:
            continue

        # output key and value
        writer.writerow ([key, value])


# UNIT TEST ----------

test_input = """\"id\"\t\"title\"\t\"tagnames\"\t\"author_id\"\t\"body\"\t\"node_type\"\t\"parent_id\"\t\"abs_parent_id\"\t\"added_at\"\t\"score\"\t\"state_string\"\t\"last_edited_id\"\t\"last_activity_by_id\"\t\"last_activity_at\"\t\"active_revision_id\"\t\"extra\"\t\"extra_ref_id\"\t\"extra_count\"\t\"marked\"
\"-\"\t\"-\"\t\"-\"\t\"001\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"only 18 fields\"\t\"-\"\t\"-\"\t\"001\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2012-02-25 08:09:06.787181+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"-\"\t\"-\"\t\"-\"\t\"002\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2012-02-27 22:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"wrong time format\"\t\"-\"\t\"-\"\t\"002\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2012-02-27 25:09:06.0+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
\"-\"\t\"-\"\t\"-\"\t\"003\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"2010-03-27 13:09:06.34523+00\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"\t\"-\"
"""

from io import StringIO
def main ():

    print ('output:')
    sys.stdin = StringIO (test_input)
    mapper ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()