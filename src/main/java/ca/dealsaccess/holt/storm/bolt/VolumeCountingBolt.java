package ca.dealsaccess.holt.storm.bolt;

import java.util.Calendar;
import java.util.Date;
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
	
	public static final String FIELD_COLUMN = "IncrementColumn";
	
	public static final String FIELD_INCREMENT = "IncrementAmount";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	public static Long getMinuteForTime(long timeStamp) {
		Calendar c = Calendar.getInstance();
		Date d = new Date(timeStamp);
		c.setTime(d);
		c.set(Calendar.SECOND,0);
		c.set(Calendar.MILLISECOND, 0);
		return c.getTimeInMillis();
	}

	@Override
	public void execute(Tuple input) {
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		collector.emit(new Values(
				getMinuteForTime(entry.getTimeStamp()), 
				1L,
				entry.getUrl()
		));
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("rowKey", "IncrementAmount", "IncrementColumn"));
	}

}
