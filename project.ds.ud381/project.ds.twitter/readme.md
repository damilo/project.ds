# Project: Visualizing Top N hashtag Tweets / Udacity Course
---


## Overview
Contains work of final project.


### Task
__Main Task__: Design and create a real-time, dynamic visualization of Tweets that contain worldwide top hashtags.

__Optional Task__: Create as many extensions as you like from any data source.


## Design
__input__: Tweets of Twitter stream

__output__: Tweets that contain top N hashtags

__atomar tasks__:

\- source Tweets (receive Tweets, store Tweets in queue)

\- determine top N hashtags (filter hashtags out of stored Tweets, store top N hashtags)

\- filter Tweets (per Tweet, compare its hashtag with top N hastags)

\- output Tweets (visualize Tweets containing top N hastags)

__topology__:

<pre>
                -- TopNBolt --
              /                \
TwitterSpout  ------------------ FilterBolt -- ReportBolt
</pre>

TopNBolt
<pre>
FilterBolt -- (Rolling)CountBolt -- IntermediateRankingsBolt -- TotalRankingsBolt
</pre>

__topology components__:

<pre>
                input                   output
TwitterSpout    stream                  text as String
FilterBolt      multi-type              filtered text as String
CountBolt       text as String          word and count as String
ReportBolt      text as String          visualized in HTML
</pre>


## additional resources:
\- <a href = 'https://storm.apache.org/'>Apache Storm</a>

\- <a href = 'https://www.vagrantup.com/'>HashiCorp Vagrant</a> for development workflow