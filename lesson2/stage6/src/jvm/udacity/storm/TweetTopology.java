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

import udacity.storm.CountBolt;
import udacity.storm.ParseTweetBolt;
import udacity.storm.ReportBolt;
import udacity.storm.TweetSpout;


class TweetTopology
{
  public static void main(String[] args) throws Exception
  {
    // create the topology
    TopologyBuilder builder = new TopologyBuilder();

    /*
     * In order to create the spout, you need to get twitter credentials
     * If you need to use Twitter firehose/Tweet stream for your idea,
     * create a set of credentials by following the instructions at
     *
     * https://dev.twitter.com/discussions/631
     *
     */

    // now create the tweet spout with the credentials
    TweetSpout tweetSpout = new TweetSpout(
        "vcsty6dNgXIQld3kOzVOl6flk",//"[Your customer key]","
        "vEMoDQLSBLa6PL5TYUtNUg70QmfOCH6bP2oT2L9D2BdVU42IsT",//"[Your secret key]",
        "35245669-lbHNqeKL29yRjwr1stgfEK0pRbYAMFGei8VzjTXfX",//"[Your access token]",
        "kfal1S7pWX691JlmAuKAI00j2CeYbujHingNzmc7CXLsK"//"[Your access secret]"
    );

    //*********************************************************************
    // Complete the Topology.
    // Part 0: attach the tweet spout to the topology - parallelism of 1
    // Part 1: // attach the parse tweet bolt, parallelism of 10 (what grouping is needed?)
    // Part 2: // attach the count bolt, parallelism of 15 (what grouping is needed?)
    // Part 3: attach the report bolt, parallelism of 1 (what grouping is needed?)
    // Submit and run the topology.


    //*********************************************************************

    builder.setBolt("tweetParse-bolt",new ParseTweetBolt(), 10).shuffleGrouping("tweet-spout");
    builder.setBolt("tweetCount-bolt",new CountBolt(), 15).fieldsGrouping("tweetParse-bolt", new Fields("tweet-word"));
    builder.setBolt("tweetReport-bolt",new ReportBolt(), 1).globalGrouping("tweetCount-bolt");

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
      cluster.submitTopology("tweet-word-count", conf, builder.createTopology());

      // let the topology run for 300 seconds. note topologies never terminate!
      Utils.sleep(300000);

      // now kill the topology
      cluster.killTopology("tweet-word-count");

      // we are done, so shutdown the local cluster
      cluster.shutdown();
    }
  }
}
