"""
summarization patterns

Task:
Write a MapReduce program that creates an index of all words
that can be found in the body of a forum post
and node id they can be found in.

Parsing of HTML not required.
"""

"""
format of data:
each line contains tab-delimited features:
'id', 'title', 'tagnames', 'author_id', 'body', 'node_type',
'parent_id', 'abs_parent_id', 'added_at', 'score', 'state_string',
'last_edited_id', 'last_activity_by_id', 'last_activity_at',
'active_revision_id', 'extra', 'extra_ref_id', 'extra_count', 'marked'

first line contains header with feature names
"""

# ----------

#!/usr/bin/python

import sys
import csv
import re

# mapper
"""
extracts words and nodes of each line of the data
key: word
value: node id
"""
def mapper ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    for line in reader:

        # extract id and body of line
        node_id = line[0]
        body = line[4].lower ()

        
        re_non_word_chars = r'[\W]'
        body_parts = re.split (re_non_word_chars, body)
        
        for part in body_parts:
            if (len (part) > 0):
                writer.writerow ([part, node_id])

# reducer
"""
gets the sorted output of mapper:
single word \t node id

outputs:
single word \t list of node ids
"""
def reducer ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    oldWord = None
    wordIndex = []
    header = False
    for line in reader:

        # jump over header
        if (header):
            header = False
            continue
        
        # check input length
        if (len (line) != 2):
            continue
        
        thisWord, thisNodeId = line

        if oldWord and oldWord != thisWord:
            out = [oldWord] + [', '.join (wordIndex)]
            writer.writerow (out)
            wordIndex = []
        
        wordIndex.append (thisNodeId)
        oldWord = thisWord
    
    out = [oldWord] + [', '.join (wordIndex)]
    writer.writerow (out)

# postproc
"""
Questions:
How many times was the word 'fantastic' used on forums?

List the nodes for the word 'fantastically'.
"""
def mapred_postproc ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)

    wordToLookFor1 = 'hello'
    wordToLookFor2 = '333'

    for line in reader:
        
        thisWord, thisNodeIds = line
        if (thisWord == wordToLookFor1):
            count = len (thisNodeIds.split (','))
            writer.writerow ([count])
        
        if (thisWord == wordToLookFor2):
            writer.writerow ([thisNodeIds])

# ----------

test_text_mapper = """\"\"\t\"\"\t\"\"\t\"\"\t\"333\"\t\"\"
\"112"\t\"\"\t\"\"\t\"\"\t\" < VerSionINg okay\"\t\"\"
\"112"\t\"\"\t\"\"\t\"\"\t\" < VerSionINg OKAY >\"\t\"\"
\"112"\t\"\"\t\"\"\t\"\"\t\" < VerSionINg oKaY < </p>\"\t\"\"
\"72"\t\"\"\t\"\"\t\"\"\t\" version?</p>\"\t\"\"
\"73"\t\"\"\t\"\"\t\"\"\t\"<p>version?</p>\"\t\"\"
\"74"\t\"\"\t\"\"\t\"\"\t\"<title>version?</title>\"\t\"\"
\"100"\t\"\"\t\"\"\t\"\"\t\" versioning\"\t\"\"
\"110"\t\"\"\t\"\"\t\"\"\t\" VerSionINg\"\t\"\"
\"112"\t\"\"\t\"\"\t\"\"\t\" < VerSionINg okay\"\t\"\"
"""
"""
\"42"\t\"\"\t\"\"\t\"\"\t\"333\"\t\"\"
\"44"\t\"\"\t\"\"\t\"\"\t\" 333 hello . miao .,!?:;"()<>[]#$=-/\"\t\"\"
"""
test_text_reducer = """\"333\"\t\"42\"
\"333\"\t\"44\"
\"hello\"\t\"44\"
\"miao\"\t\"44\"
"""

test_text_postproc = """\"333\"\t\"42, 44\"
\"hello\"\t\"44, 32, 112\"
\"miao\"\t\"44\"
"""


from io import StringIO
def main ():

    print ('mapper out:')
    sys.stdin = StringIO (test_text_mapper)
    mapper ()

    print ('reducer out:')
    sys.stdin = StringIO (test_text_reducer)
    reducer ()

    print ('postproc out:')
    sys.stdin = StringIO (test_text_postproc)
    mapred_postproc ()

    sys.stdin = sys.__stdin__

if __name__ == '__main__':
    main ()

# ----------

"""
old mapper - deprecated
def mapper ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    header = True
    for line in reader:

        # jump over header
        if (header):
            header = False
            continue
        
        # check input length
        # we assume that every line is okay

        # extract id and body of line
        node_id = line[0]
        body = line[4]

        # split body into single words
        # split at whitespaces and '.,!?:;"()<>[]#$=-/'
        body_parts = body.strip ().split ()

        body_parts_clean = []
        look_for_non_word_chars = r'[\W]'
        for part in body_parts:
            cleaned_part = part
            result = re.findall (look_for_non_word_chars, part)
            if result:
                # look for HTML
                # beware: this is an absolute dirty version, better take an HTML parser instead of regex (i.e. BeautifulSoup)
                last_html_end = 0
                for html_start in re.finditer (r'<', part):
                    html_end = re.search (r'>', part[html_start.start ():])
                    if not html_end:
                        continue
                    this_html_start = html_start.start () - last_html_end
                    this_html_end = html_start.start () + html_end.start () + 1
                    part = part.replace (part[this_html_start:this_html_end], '')
                    last_html_end = this_html_end
                
                cleaned_part = re.sub (look_for_non_word_chars, '', part)

            if (len (cleaned_part) > 0):
                body_parts_clean.append (cleaned_part.lower ())
        
        # output key-value, ie. single word \t node id
        for part in body_parts_clean:
            out = [part] + [node_id]
            writer.writerow (out)
"""