# In this exercise, we are interested in the field 'body' (which is the 5th field, 
# line[4]). The objective is to count the number of forum nodes where 'body' either 
# contains none of the three punctuation marks: period ('.'), exclamation point ('!'), 
# question mark ('?'), or else 'body' contains exactly one such punctuation mark as the 
# last character. There is no need to parse the HTML inside 'body'. Also, do not pay
# special attention to newline characters.

#!/usr/bin/python

import sys
import csv
import re

def mapper ():
    reader = csv.reader (sys.stdin, delimiter='\t')
    writer = csv.writer (sys.stdout, delimiter='\t', quotechar='"', quoting=csv.QUOTE_ALL)
    
    num_no_pm = 0
    num_pm_last_char = 0
    for line in reader:

        body = line[4]
        if (re.findall (r'\n', body)):
            body = re.sub (r'\n', ' ', body)
        
        result = re.findall (r'[.!?]', body)

        # no punctuation mark at all
        if not result:
            num_no_pm += 1
            writer.writerow (line)
        
        # exactly one punctuation mark
        elif len (result) == 1: 
            result = re.search (r'[.!?]$', body)
            if result: # punctuation mark is last character
                num_pm_last_char += 1
                writer.writerow (line)
    
    return num_no_pm, num_pm_last_char


test_text = """\"\"\t\"\"\t\"\"\t\"\"\t\"This is one sentence\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"Also one sentence!\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"Hey!\nTwo sentences!\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"One. Two! Three?\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"One Period. Two Sentences\"\t\"\"
\"\"\t\"\"\t\"\"\t\"\"\t\"Three\nlines, one sentence\n\"\t\"\"
"""

# This function allows you to test the mapper with the provided test string
def main ():
    from io import StringIO
    sys.stdin = StringIO (test_text)
    print (mapper ())
    sys.stdin = sys.__stdin__

if __name__ == "__main__":
    main ()
