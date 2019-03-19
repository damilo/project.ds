package storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import storm.spout.*;
import storm.bolt.*;

/**
 * start topology with: storm jar target/storm.twitter-0.0.1-jar-with-dependencies.jar storm.Topology
 * hit CTRL + C when topology shut down
*/

public class Topology {

    public static final String Name = "project-ds-twitter";
    
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

        // tweet parser bolt
        builder.setBolt (ParseTweetBolt.Name, new ParseTweetBolt (), 3)
            .shuffleGrouping (TweetSpout.Name);
        
        builder.setBolt (CountBolt.Name, new CountBolt (), 3)
            .fieldsGrouping (ParseTweetBolt.Name, new Fields ("tweet-hashtag"));

        // report bolt to publish results
        builder.setBolt (ReportBolt.Name, new ReportBolt (), 1)
            .globalGrouping (CountBolt.Name);


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