package storm.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import storm.Topology;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * A bolt that publishes results to redis
 */
public class ReportBolt extends BaseRichBolt {
    
    public static final String Name = "bolt-reporter";

    // place holder to keep the connection to redis
    transient RedisConnection<String,String> redis;

    @Override
    public void prepare (Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // instantiate a redis connection
        RedisClient client = new RedisClient ("localhost", 6379);
        // initiate the actual connection
        redis = client.connect ();
    }

    @Override
    public void execute (Tuple tuple) {
        // access the first column 'sentence'
        String sentence = tuple.getString (0);
        // access the second column 'count'
        Long count = tuple.getLongByField ("count");
        // publish the word count to redis using word as the key
        redis.publish ("project.ds.twitter", sentence + "|" + Long.toString (count));
    }

    public void declareOutputFields (OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }
}