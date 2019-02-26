# Project: Intro to Hadoop and MapReduce / Udacity Course
---


## Overview
Contains work of course projects.

All work has been done on a virtual machine with a CDH framework installed (Cloudera Distribution for Hadoop).

Installation instructions: <a href='https://d20vrrgs8k4bvw.cloudfront.net/documents/en-IN/BigDataVM.pdf'>Udacity BigDataVM</a>


## dir: l6-project

data: tab delimited purchases with features date, time, store, item, cost, payment / 202 MiB

Hadoop mapper and reducer scripts to solve tasks:

\- sales breakdown by product category across all stores

\- monetary value for the highest individual sale for each separate store

\- total sales value across all the stores, and the total number of sales

## dir: l7-mapredpatts

Scripts for various tasks to learn common MapReduce design patterns.

## dir: project.ds.udforum

Final project

data: Udacitiy discussion forum data

\- forum_node.tsv: 19 fields, tab delimited, field data in double quotes, header present, field names: 'id', 'title', 'tagnames', 'author_id', 'body', 'node_type', 'parent_id', 'abs_parent_id', 'added_at', 'score', 'state_string', 'last_edited_id', 'last_activity_by_id', 'last_activity_at', 'active_revision_id', 'extra', 'extra_ref_id', 'extra_count', 'marked'

\- forum_users.tsv: 5 fields, tab delimited, field data in double quotes, header present, field names: 'user_ptr_id', 'reputation', 'gold', 'silver', 'bronze'

Tasks:

\- 01 students posting time: For each student, find the hour during which the student has been posted the most posts.

\- 02 post and answer length: Output the length of a post and the average answer length (just answer, not comment) for each post.

\- 03 top tags: Output top 10 tags, ordered by the number of questions they appear in.

\- 04 study groups: For each forum thread (that is a question node with all it's answers and comments), give a list of students that have posted there.

## additional resources:

\- Python regex: <a href = 'https://stackabuse.com/using-regex-for-text-manipulation-in-python/'>Regex for text manipulation in Python</a>

\- transfer of text: <a href = 'https://pastebin.com/'>pastebin</a>

\- transfer of files: <a href = 'https://justbeamit.com/'>justbeamit</a>