#!/usr/bin/python
"""
Your mapper function should print out 10 lines containing longest posts, sorted in
ascending order from shortest to longest.
Please do not use global variables and do not change the "main" function.
"""
import sys
import csv

def keyFunc (e):
    return len (e[4])


def mapper():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)

    post_list = []
    output_list = []
    k = 10
    for line in reader:

        # first of all fill our list with at least k elements
        if (len (post_list) < k):
            post_list.append (line)
            post_list.sort (key = keyFunc)
        # if list has 10 elements,
        # then check if post of current line is longer than one of the post list members
        # if so, then replace member by current line
        else:
            len_thisPost = len (line[4])
            for i in range (k):
                len_listPost = len (post_list[i])
                if (len_thisPost > len_listPost):
                    post_list[i] = line
                    post_list.sort (key = keyFunc)
                    break
    
    # take top 10 posts from the end
    output_list = post_list[-10:]

    for i in range (k):
        writer.writerow (output_list[i])


test_text = """\"\"\t\"\"\t\"\"\t\"\"\t\"333\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"88888888\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"1\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"11111111111\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"1000000000\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"22\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"4444\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"666666\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"55555\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"999999999\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"7777777\"\t\"\"
"""
# additional rows to see that sort after member replacement (else path) is necessary
"""
\"\"\t\"\"\t\"\"\t\"\"\t\"1111111111123\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"111111111112\"\t\"\"
"""


# This function allows you to test the mapper with the provided test string
from io import StringIO

def main ():
    sys.stdin = StringIO (test_text)
    mapper ()
    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()
