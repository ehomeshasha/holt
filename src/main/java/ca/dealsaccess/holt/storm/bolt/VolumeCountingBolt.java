package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class VolumeCountingBolt extends BaseRichBolt {

	public static Logger LOG = LoggerFactory.getLogger(VolumeCountingBolt.class);
	private OutputCollector collector;
	
	
	
	public static final String FIELD_ROW_KEY = "rowKey";
	
	public static final String FIELD_INCREMENT = "IncrementAmount";
	
	public static final String FIELD_COLUMN_ALL = "all";
	
	public static final String FIELD_COLUMN_URL = "url";
	
	public static final String FIELD_COLUMN_EXT = "extension";
	
	public static final String FIELD_COLUMN_IP = "ip";
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	

	@Override
	public void execute(Tuple input) {
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		collector.emit(new Values(
				entry.getMinuteForTime(), 
				1L,
				"all",
				entry.getUrl(),
				entry.getExtension(),
				entry.getIp()
		));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				FIELD_ROW_KEY, 
				FIELD_INCREMENT, 
				FIELD_COLUMN_ALL, 
				FIELD_COLUMN_URL, 
				FIELD_COLUMN_EXT, 
				FIELD_COLUMN_IP));
		//declarer.declare(new Fields(FIELD_ROW_KEY, FIELD_INCREMENT));
	}

}
