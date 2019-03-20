package topntweets;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import topntweets.bolt.*;
import topntweets.spout.*;
import topntweets.bolt.topn.IntermediateRankingsBolt;
import topntweets.bolt.topn.TotalRankingsBolt;

/**
 * start topology with: storm jar target/storm.twitter-0.0.1-jar-with-dependencies.jar topntweets.Topology
 * hit CTRL + C when topology shut down
*/

public class Topology {

    public static final String Name = "topn-tweets";
    
    public static void main (String[] args) throws Exception {
        
        TopologyBuilder builder = new TopologyBuilder ();
        
        // spout
        TweetSpout tweetSpout = new TweetSpout (
            "(API key)",
            "(API secret key)",
            "(Access token)",
            "(Access token secret)"
        );
        builder.setSpout (TweetSpout.Name, tweetSpout, 1);
        
        // top N bolt
        builder.setBolt (ParseTweetBolt.Name, new ParseTweetBolt (), 5)
            .shuffleGrouping (TweetSpout.Name);
        
        builder.setBolt (CountBolt.Name, new CountBolt (), 10)
            .fieldsGrouping (ParseTweetBolt.Name, new Fields ("tweet-hashtag"));
        
        builder.setBolt (IntermediateRankingsBolt.Name, new IntermediateRankingsBolt (), 5)
            .fieldsGrouping (CountBolt.Name, new Fields ("word"));
        
        builder.setBolt (TotalRankingsBolt.Name, new TotalRankingsBolt (10), 1)
            .globalGrouping (IntermediateRankingsBolt.Name);

        // ...
        builder.setBolt (ParseTweetBolt.Name + "-full-tweet", new ParseTweetBolt (), 1)
            .globalGrouping (TweetSpout.Name)
            .globalGrouping (TotalRankingsBolt.Name);
        
        // report bolt to publish results
        builder.setBolt (ReportBolt.Name, new ReportBolt (), 1)
            .globalGrouping (ParseTweetBolt.Name + "-full-tweet");

        
        // create default config
        Config conf = new Config ();
        conf.setDebug (true);

        
        System.out.printf ("[i] topology %s starting...\n", Topology.Name);
        
        // run it in a simulated local cluster
        LocalCluster cluster = new LocalCluster ();
        cluster.submitTopology (Topology.Name, conf, builder.createTopology ());

        // let the topology run for 4 * 30 seconds
        // [i] topologies never terminate
        Thread.sleep (30000*4);

        // kill the topology
        cluster.killTopology (Topology.Name);
        cluster.shutdown ();

        System.out.printf ("[i] topology %s shut down...\n", Topology.Name);
    }
}