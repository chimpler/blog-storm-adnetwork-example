package com.chimpler.adnetwork.spout;

import java.util.Map;
import java.util.Random;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RandomImpressionTupleSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Random random;

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		this.random = new Random();
		
	}

	@Override
	public void nextTuple() {
		long timestamp = System.currentTimeMillis() / 1000;
		int publisherId = random.nextInt(10);
		int countryId = random.nextInt(10);
		int cookieId = random.nextInt(10000);
		
		collector.emit(new Values(timestamp, publisherId,
								  countryId, cookieId));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "publisher_id", "country_id", "cookie_id"));
	}
}