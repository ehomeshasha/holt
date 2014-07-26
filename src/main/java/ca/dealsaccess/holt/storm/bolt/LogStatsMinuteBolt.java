package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.log.AbstractLogStats;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.ApacheLogStats;
import ca.dealsaccess.holt.log.LogConstants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class LogStatsMinuteBolt extends BaseRichBolt {

	public static Logger LOG = LoggerFactory.getLogger(LogStatsMinuteBolt.class);
	
	private OutputCollector collector;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}

	

	@Override
	public void execute(Tuple input) {
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		AbstractLogStats logStats = new ApacheLogStats(entry, collector);
		logStats.setDuration(AbstractLogStats.MINUTE);
		logStats.emit();
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(
				AbstractLogStats.FIELD_ROW_KEY, 
				AbstractLogStats.FIELD_INCREMENT,
				//AbstractLogStats.FIELD_COLUMN_ALL,
				AbstractLogStats.FIELD_COLUMN_IP,
				//AbstractLogStats.FIELD_COLUMN_URL,
				//AbstractLogStats.FIELD_COLUMN_METHOD,
				//AbstractLogStats.FIELD_COLUMN_PROTOCOL,
				AbstractLogStats.FIELD_COLUMN_EXT
				//AbstractLogStats.FIELD_COLUMN_STATUSCODE,
				//AbstractLogStats.FIELD_COLUMN_RESPONSESIZE,
				//AbstractLogStats.FIELD_COLUMN_COUNTRY,
				//AbstractLogStats.FIELD_COLUMN_CITY
			)
		);
	}

}
