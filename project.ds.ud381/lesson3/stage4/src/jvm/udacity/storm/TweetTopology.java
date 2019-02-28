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

class TweetTopology {
  public static void main (String[] args) throws Exception {
    
    // create the topology
    TopologyBuilder builder = new TopologyBuilder ();

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout (
      "[Your customer key]",
      "[Your secret key]",
      "[Your access token]",
      "[Your access secret]"
    );

    // attach the tweet spout to the topology - parallelism of 1
    builder.setSpout ("tweet-spout", tweetSpout, 1);

    // attach the parse tweet bolt using shuffle grouping
    builder.setBolt ("parse-tweet-bolt", new ParseTweetBolt(), 10)
              .shuffleGrouping ("tweet-spout");

    // attach the count bolt using fields grouping - parallelism of 15
    builder.setBolt ("count-bolt", new CountBolt (), 15)
              .fieldsGrouping ("parse-tweet-bolt", new Fields ("tweet-word"));

    // ranking
    builder.setBolt ("intermediate-rankings", new IntermediateRankingsBolt (), 5)
              .fieldsGrouping ("count-bolt", new Fields ("word"));
    
    builder.setBolt ("total-rankings", new TotalRankingsBolt (10), 1)
              .globalGrouping ("intermediate-rankings");

    // attach the report bolt using global grouping - parallelism of 1
    builder.setBolt ("report-bolt", new ReportBolt (), 1)
              .globalGrouping ("total-rankings");

    // create the default config object
    Config conf = new Config ();

    // set the config in debugging mode
    conf.setDebug (true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers (3);

      // create the topology and submit with config
      StormSubmitter.submitTopology (args[0], conf, builder.createTopology ());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism (3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster ();

      // submit the topology to the local cluster
      cluster.submitTopology ("tweet-topk-hashtags", conf, builder.createTopology ());

      // let the topology run for 10 * 30 seconds. note topologies never terminate!
      Utils.sleep (10 * 30000);

      // now kill the topology
      cluster.killTopology ("tweet-topk-hashtags");

      // we are done, so shutdown the local cluster
      cluster.shutdown ();
    }
  }
}