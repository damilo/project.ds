package udacity.storm.bolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

import udacity.storm.TopologyComponents;

/**
 * A bolt that adds the exclamation marks '!!!' to word
 */
public class ExclamationBolt extends BaseRichBolt {
  // To output tuples from this bolt to the next stage bolts, if any
  OutputCollector _collector;

  private Map<String, String> _favoritesMap = null;

  @Override
  public void prepare (Map map, TopologyContext topologyContext, OutputCollector collector) {
    // save the output collector for emitting tuples
    _collector = collector;

    _favoritesMap = new HashMap<String, String> ();
  }

  @Override
  public void execute (Tuple tuple) {

    String sourceComponent = tuple.getSourceComponent ();
    String sentence = null;

    if (sourceComponent.equals (TopologyComponents.MyLikesSpout)) {
      _favoritesMap.put (tuple.getStringByField ("name"), tuple.getStringByField ("favorite"));
    }

    if (sourceComponent.equals (TopologyComponents.MyNamesSpout)) {
      String name = tuple.getStringByField ("name");
      if (_favoritesMap.containsKey (name)) {
        String favorite = _favoritesMap.get (name);
        sentence = String.format ("%s's favorite is %s", name, favorite);
      }
    }

    if (sourceComponent.equals (TopologyComponents.ExclamationBolt)) {
      sentence = tuple.getString (0);
    }

    // build the word with the exclamation marks appended
    if (sentence != null) {
      StringBuilder exclamatedSentence = new StringBuilder ();
      exclamatedSentence.append (sentence).append ("!!!");

      // emit the word with exclamations
      _collector.emit (tuple, new Values (exclamatedSentence.toString()));
    }
  }

  @Override
  public void declareOutputFields (OutputFieldsDeclarer declarer) {
    // tell storm the schema of the output tuple for this spout

    // tuple consists of a single column called 'exclamated-word'
    declarer.declare(new Fields("exclamated-word"));
  }
}