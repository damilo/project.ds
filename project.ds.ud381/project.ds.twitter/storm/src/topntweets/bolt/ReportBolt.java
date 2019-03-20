package topntweets.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

import topntweets.bolt.topn.tools.Rankable;
import topntweets.bolt.topn.tools.Rankings;

/**
 * A bolt that publishes results to redis
 * <p/>
 * emits: nothing
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

        if (tuple.getSourceComponent().equals (ParseTweetBolt.Name + "-full-tweet")) {
            // tweet starts with "toReportBolt-"
            String tweet = tuple.getString (0);
            if (tweet.startsWith ("toReportBolt-")) {
                Integer count = 15; //tuple.getIntegerByField ("count");
                // publish the word count to redis using word as the key
                redis.publish ("topn-tweets", tweet.substring ("toReportBolt-".length()) + "|" + Integer.toString (count));
            }
        }
        
        // Rankings rankableList = (Rankings) tuple.getValue (0);
        // for (Rankable r: rankableList.getRankings ()) {
        //     String word = r.getObject ().toString ();
        //     Long count = r.getCount ();
        //     redis.publish ("topn-tweets", word + "|" + Long.toString (count));
        // }

        // // access the first column 'sentence'
        // String tweet = tuple.getString (0);
        // // access the second column 'count'
        // Integer count = 20; //tuple.getIntegerByField ("count");
        // // publish the word count to redis using word as the key
        // redis.publish ("topn-tweets", tweet + "|" + Integer.toString (count));
    }

    public void declareOutputFields (OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }
}