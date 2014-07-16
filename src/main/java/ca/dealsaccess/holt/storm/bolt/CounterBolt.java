package ca.dealsaccess.holt.storm.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class CounterBolt extends BaseBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1879997855714497018L;

	private static long counter = 0;
	
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		System.out.println("msg = "+tuple.getString(0)+" -------------- counter = "+(counter++));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		

	}

}
