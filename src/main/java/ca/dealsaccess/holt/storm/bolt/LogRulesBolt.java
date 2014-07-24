package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class LogRulesBolt extends BaseRichBolt {

	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void execute(Tuple input) {
		String logText = (String) input.getValueByField(LogConstants.LOG_TEXT);
		ApacheLogEntry logEntry = new ApacheLogEntry(logText);
		logEntry.parseLogText();
		collector.emit(new Values(logEntry));
		
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(LogConstants.LOG_ENTRY));
	}

}
