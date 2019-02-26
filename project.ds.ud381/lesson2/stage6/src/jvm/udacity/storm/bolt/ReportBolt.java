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

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.StallWarning;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;


/**
* A bolt that prints the word and count to redis
*/
public class ReportBolt extends BaseRichBolt {
    // place holder to keep the connection to redis
    transient RedisConnection<String,String> redis;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         outputCollector)
    {
        // instantiate a redis connection
        RedisClient client = new RedisClient("localhost",6379);

        // initiate the actual connection
        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple)
    {
        // access the first column 'word'
        String word = tuple.getStringByField("obj");

        // access the second column 'count'
        Integer count = tuple.getIntegerByField("count");

        // publish the word count to redis using word as the key
        redis.publish("WordCountTopology", word + "|" + Long.toString(count));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        // nothing to add - since it is the final bolt
    }
}