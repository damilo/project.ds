package udacity.storm.bolt;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;

import java.util.HashMap;
import java.util.Map;

/**
* A bolt that counts the words that it receives
*/
public class CountBolt extends BaseRichBolt {

    // To output tuples from this bolt to the next stage bolts, if any
    private OutputCollector collector;

    // Map to store the count of the words
    private Map<String, Integer> countMap;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         outputCollector)
    {

        // save the collector for emitting tuples
        collector = outputCollector;

        // create and initialize the map
        countMap = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple)
    {
        // input: words from preceeding split bolt

        //**************************************************
        //BEGIN YOUR CODE - Part 1a
        //Check if incoming word is in countMap.  If word does not
        //exist then add word with count = 1, if word exist then
        //increment count.

        //Syntax to get the word from the 1st column of incoming tuple
        String word = tuple.getString (0);

        // check if the word is present in the map
        if (countMap.get(word) == null) {
            countMap.put(word, 1);
        }
        else {
            Integer val = countMap.get (word);
            countMap.put (word, ++val);
        }

        //After countMap is updated, emit word and count to output collector
        // Syntax to emit the word and count (uncomment to emit)
        collector.emit (new Values (word, countMap.get (word)));

        //END YOUR CODE
        //***************************************************
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a two columns called 'word' and 'count'

        // declare the first column 'word', second colmun 'count'

        //****************************************************
        //BEGIN YOUR CODE - part 1b
        //uncomment line below to declare output

        outputFieldsDeclarer.declare (new Fields ("word", "count"));

        //END YOUR CODE
        //****************************************************
    }
}