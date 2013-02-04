package com.chimpler.adnetwork;

import com.chimpler.adnetwork.bolt.AggregateByTimeAndPersistBolt;
import com.chimpler.adnetwork.spout.RandomImpressionTupleSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class Main {
	public static void main(String[] args) throws Exception {
		TopologyBuilder builder = new TopologyBuilder();
		
		/*
		 * we aggregate as follows:
		 * - minute: 10 secs
		 * - hour: 30 secs
		 * - day: 60 secs
		 * for the purpose of our example
		 */
		
		builder.setSpout("ImpressionLogSpout", new RandomImpressionTupleSpout());
		builder.setBolt("AggByMinuteBolt", new AggregateByTimeAndPersistBolt(10)).fieldsGrouping("ImpressionLogSpout", new Fields("publisher_id"));
		builder.setBolt("AggByHourBolt", new AggregateByTimeAndPersistBolt(30)).fieldsGrouping("ImpressionLogSpout", new Fields("publisher_id"));
		builder.setBolt("AggByDayBolt", new AggregateByTimeAndPersistBolt(60)).fieldsGrouping("ImpressionLogSpout", new Fields("publisher_id"));

		Config conf = new Config();
		conf.setDebug(true);
		
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("aggImps", conf, builder.createTopology());
        Thread.sleep(1000000);
        cluster.shutdown();
	}
}
