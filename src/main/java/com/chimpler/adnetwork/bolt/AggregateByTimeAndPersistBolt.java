package com.chimpler.adnetwork.bolt;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class AggregateByTimeAndPersistBolt implements IRichBolt {
	private Map<Integer, Accumulator> accumulators;
	private int aggregationTime;
	private Mongo mongo;
	private DBCollection collection;

	public AggregateByTimeAndPersistBolt(int aggregationTime) {
		this.aggregationTime = aggregationTime;
	}
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.accumulators = new HashMap<Integer, Accumulator>();
		try {
			mongo = new Mongo("localhost", 27017);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	    DB db = mongo.getDB("adnetwork_test");
	    collection = db.getCollection("report_data_" + aggregationTime);
	}

	@Override
	public void execute(Tuple input) {
		int timestamp = input.getIntegerByField("timestamp");
		int publisherId = input.getIntegerByField("publisher_id");
		int cookieId = input.getIntegerByField("cookie_id");
		
		// round time by aggregationTime
		int timestampSlice = (timestamp / aggregationTime) * aggregationTime;

		Accumulator accumulator = accumulators.get(publisherId);
		if (accumulator == null) {
			accumulator = new Accumulator();
			accumulators.put(publisherId, accumulator);
		} else {
			// if we receive a new tuple that has a timestamp in another time slice, persist previous data
			int lastTimestampSlice = accumulator.getLastTimestamp();
					
			if (lastTimestampSlice != timestampSlice) {
				persistLastSlice(publisherId);
			}
		}		
		
		accumulator.add(cookieId, timestampSlice);
	}
	
	private void persistLastSlice(int publisherId) {
		Accumulator accumulator = accumulators.get(publisherId);

		DBObject obj = new BasicDBObject();
		int timestamp = accumulator.getLastTimestamp();  
		int count = accumulator.getCounter();
		int uniques = accumulator.getCookies().size();

		obj.put("_id", publisherId + "-" + timestamp);
		obj.put("publisher_id", publisherId);
		obj.put("timestamp", timestamp);
		obj.put("count", count);
		obj.put("uniques", uniques);
		collection.save(obj);
		
		accumulators.put(publisherId, null);
	}

	@Override
	public void cleanup() {
		mongo.close();
		for (int publisherId: accumulators.keySet()) {
			persistLastSlice(publisherId);
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public class Accumulator {
		private int counter;
		private Set<Integer> cookies;
		private int lastTimestamp;
		
		public Accumulator() {
			this.counter = 0;
			this.cookies = new HashSet<Integer>();
		}
		
		public void add(int cookieId, int lastTimestamp) {
			counter++;
			cookies.add(cookieId);
			this.lastTimestamp = lastTimestamp;
		}

		public int getCounter() {
			return counter;
		}

		public void setCounter(int counter) {
			this.counter = counter;
		}

		public Set<Integer> getCookies() {
			return cookies;
		}

		public void setCookies(Set<Integer> cookies) {
			this.cookies = cookies;
		}

		public int getLastTimestamp() {
			return lastTimestamp;
		}

		public void setLastTimestamp(int lastTimestamp) {
			this.lastTimestamp = lastTimestamp;
		}
	}
}
