package udacity.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import backtype.storm.utils.Utils;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

//********* TO DO part 1-of-2 import RandomSentenceSpout similar to lesson 1
import udacity.storm.spout.RandomSentenceSpout;
//********* END TO DO part 1-of-2

import udacity.storm.bolt.SplitBolt;
import udacity.storm.bolt.CountBolt;
import udacity.storm.bolt.ReportBolt;

/**
 * This topology demonstrates how to count distinct words from
 * a stream of words.
 *
 * This is an example for Udacity Real Time Analytics Course - ud381
 *
 */
public class SentenceWordCountTopology {

  /**
   * Constructor - does nothing
   */

   //Note: Constructor must match class name
  private SentenceWordCountTopology () { }


  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    //***** TO DO part 2-of-2 remove WordSpout and change to RandomSentenceSpout

    // attach sentence spout to the topology - parallelism of 1
    builder.setSpout ("sentence-spout", new RandomSentenceSpout (), 1);

    builder.setBolt ("split-bolt", new SplitBolt (), 5).shuffleGrouping ("sentence-spout");

    // attach the count bolt using fields grouping - parallelism of 15
    builder.setBolt ("count-bolt", new CountBolt (), 5).fieldsGrouping ("split-bolt", new Fields ("word"));

    //***** END part 2-of-2 remove*************************************

    // attach the report bolt using global grouping - parallelism of 1
    //***************************************************
    // BEGIN YOUR CODE - part 2

    builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping ("count-bolt");


    // END YOUR CODE
    //***************************************************

    // create the default config object
    Config conf = new Config();

    // set the config in debugging mode
    conf.setDebug(true);

    if (args != null && args.length > 0) {

      // run it in a live cluster

      // set the number of workers for running all spout and bolt tasks
      conf.setNumWorkers(3);

      // create the topology and submit with config
      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

    } else {

      // run it in a simulated local cluster

      // set the number of threads to run - similar to setting number of workers in live cluster
      conf.setMaxTaskParallelism(3);

      // create the local cluster instance
      LocalCluster cluster = new LocalCluster();

      // submit the topology to the local cluster
      // name topology
      cluster.submitTopology("sentence-count", conf, builder.createTopology());

      //**********************************************************************
      // let the topology run for 30 seconds. note topologies never terminate!
      Thread.sleep(30000);
      //**********************************************************************

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
