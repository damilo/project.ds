package udacity.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

//import backtype.storm.utils.Utils;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;

import java.util.Map;

/**
* A bolt that counts the words that it receives
*/
public class SplitBolt extends BaseRichBolt {

    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;

    @Override
    public void prepare (Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // save the collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute (Tuple tuple) {
        // input: sentence from preceeding spout

        // read in sentence
        String sentence = tuple.getString (0);

        // split up sentence into words
        String delims = "[ .,?!]+";
        String[] words = sentence.split (delims);

        // emit words
        for (String word: words) {
            collector.emit (new Values (word));
        }
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        outputFieldsDeclarer.declare (new Fields ("word"));
    }
}