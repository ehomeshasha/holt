package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class LogStatsIPBolt extends BaseRichBolt {

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		
	}

	@Override
	public void execute(Tuple input) {

		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		
	}

}
