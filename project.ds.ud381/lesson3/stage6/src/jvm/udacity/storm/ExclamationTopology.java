package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.HashMap;
import java.util.Map;

//******* Import MyLikesSpout and MyNamesSpout
import udacity.storm.spout.*;
import udacity.storm.bolt.*;


/**
 * This is a basic example of a storm topology.
 *
 * This topology demonstrates how to add three exclamation marks '!!!'
 * to each word emitted
 *
 * This is an example for Udacity Real Time Analytics Course - ud381
 *
 */
public class ExclamationTopology {

  public static void main (String[] args) throws Exception {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder ();

    // attach the word spout to the topology - parallelism of 10
    builder.setSpout (TopologyComponents.MyLikesSpout, new MyLikesSpout (), 10);
    builder.setSpout (TopologyComponents.MyNamesSpout, new MyNamesSpout (), 10);

    // attach the exclamation bolt to the topology - parallelism of 3
    builder.setBolt (TopologyComponents.ExclamationBolt, new ExclamationBolt (), 3)
              .shuffleGrouping (TopologyComponents.MyLikesSpout)
              .shuffleGrouping (TopologyComponents.MyNamesSpout);
    
    // attach the exclamation bolt to the topology - parallelism of 3
    builder.setBolt (TopologyComponents.ExclamationBolt2, new ExclamationBolt (), 2)
              .shuffleGrouping (TopologyComponents.ExclamationBolt);

    builder.setBolt (TopologyComponents.ReportBolt, new ReportBolt (), 1)
              .globalGrouping (TopologyComponents.ExclamationBolt2);

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

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster ();

      // submit the topology to the local cluster
      cluster.submitTopology (TopologyComponents.Topology, conf, builder.createTopology ());

      // let the topology run for 30 seconds. note topologies never terminate!
      Thread.sleep (30000);

      // kill the topology
      cluster.killTopology (TopologyComponents.Topology);

      // we are done, so shutdown the local cluster
      cluster.shutdown ();
    }
  }
}