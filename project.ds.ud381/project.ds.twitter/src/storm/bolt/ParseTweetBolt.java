package storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * A bolt that parses the tweet into words, emits: tweet-hashtag
 */
public class ParseTweetBolt extends BaseRichBolt {

    public static final String Name = "bolt-tweet-parser";

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    @Override
    public void prepare (Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // get the 1st column 'tweet' from tuple
        String tweet = tuple.getString (0);

        // provide the delimiters for splitting the tweet
        String delims = "[ .,?!]+";
        // now split the tweet into tokens
        String[] tokens = tweet.split (delims);

        // emit hashtags only
        // criteria 1: starts with '#'
        // criteria 2: word length > 2, will throw away words as "be", "as" < is general, but okay
        for (String token: tokens) {
          if (token.startsWith ("#") && token.length () > 2) {
            collector.emit (new Values (token));
          }
        }
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer) {
        declarer.declare (new Fields ("tweet-hashtag"));
    }
}