package topntweets.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

import topntweets.bolt.topn.TotalRankingsBolt;
import topntweets.spout.TweetSpout;
import topntweets.bolt.topn.tools.Rankable;
import topntweets.bolt.topn.tools.Rankings;

import java.util.Arrays;

/**
 * A bolt that parses the tweet into words
 * <p/>
 * emits: tweet-hashtag (String) or tweet (String) if source is total ranker
 */
public class ParseTweetBolt extends BaseRichBolt {

    public static final String Name = "bolt-tweet-parser";

    // stop word list
    // [i] "rt"/"RT" stands for re-tweet
    private static final String[] stopWords = {"omg", "wtf", "bam", "rofl", "lol", "idgaf", "love", "rt"};

    private Rankings currentRankings = null;

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare (Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // get source
        String sourceComponent = tuple.getSourceComponent ();

        // from total ranker
        if (sourceComponent.equals (TotalRankingsBolt.Name)) {
            // save the current rankings list containing the top N hashtags only
            currentRankings = (Rankings) tuple.getValue (0);
            System.out.println ("[i] rankings list updated");
        }

        // new tweet from spout arrives - needs to be parsed
        if (sourceComponent.equals (TweetSpout.Name)) {
            // get the 1st column 'tweet' from tuple
            String tweet = tuple.getString (0);
            // provide the delimiters for splitting the tweet
            String delims = "[ .,?!]+";
            // now split the tweet into tokens
            String[] tokens = tweet.split (delims);

            // emit hashtags only
            // criteria 1: starts with '#'
            // criteria 2: word length > 3, will throw away words as "#be", "#as", "#WC" < is general approach, but okay
            // criteria 3: word not in stop words list
            for (String token: tokens) {
                
                // check if token is in rankings list - if so, emit whole tweet to reporter
                if (currentRankings != null) {
                    for (Rankable r: currentRankings.getRankings ()) {
                        String word = r.getObject ().toString ();
                        if (word.equals (token)) {
                            collector.emit (new Values ("toReportBolt-" + tweet));
                            return;
                        }
                    }
                }

                // otherwise emit to counter
                if (token.startsWith ("#") && token.length () > 3 && !Arrays.asList (stopWords).contains (token.toLowerCase ())) {
                    collector.emit (new Values (token));
                }
            }
        }
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer) {
        declarer.declare (new Fields ("tweet-hashtag"));
    }
}