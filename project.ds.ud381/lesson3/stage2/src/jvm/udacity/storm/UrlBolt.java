package udacity.storm;

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

import java.io.IOException;
import java.util.Map;

//import javax.swing.text.Document;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

/**
 * A bolt that parses the tweet into words
 */
public class UrlBolt extends BaseRichBolt {
  // To output tuples from this bolt to the count bolt
  OutputCollector collector;

  @Override
  public void prepare (Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    // save the output collector for emitting tuples
    collector = outputCollector;
  }

  @Override
  public void execute (Tuple tuple) {
    // get the 1st column 'tweet-url' from tuple
    // example of a 'tweet-url': https://t.co/4lO38ve4tgH6wr ([i] page doesn't exist due to data protection rules)
    String tweetUrl = tuple.getString (0);
    try {
      Document doc = Jsoup.connect (tweetUrl).get ();
      String title = doc.title ();
      
      // title is a new URL
      Document realDoc = Jsoup.connect (title).get ();
      String realTitle = realDoc.title ();
      
      collector.emit (new Values (realTitle));
    } catch (IOException ioe) { }
  }

  @Override
  public void declareOutputFields (OutputFieldsDeclarer declarer) {
    // tell storm the schema of the output tuple for this spout
    // tuple consists of a single column called 'tweet-word'
    declarer.declare (new Fields ("tweet-url-title"));
  }
}