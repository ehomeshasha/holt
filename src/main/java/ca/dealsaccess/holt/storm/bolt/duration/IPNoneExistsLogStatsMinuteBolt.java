package ca.dealsaccess.holt.storm.bolt.duration;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
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
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class IPNoneExistsLogStatsMinuteBolt extends BaseRichBolt {

	public static Logger LOG = LoggerFactory.getLogger(IPNoneExistsLogStatsMinuteBolt.class);
	
	private OutputCollector collector;
	
	private AstyanaxCnxn astyanaxCnxn;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		try {
			astyanaxCnxn = new AstyanaxCnxn();
			astyanaxCnxn.connect();
		} catch (Exception e) {
			LOG.error("cannot connect to cassandra");
		}
	}

	@Override
	public void execute(Tuple input) {
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.MINUTE_LOG_ENTRY);
		AbstractLogStats logStats = new ApacheLogStats(entry, collector, input, astyanaxCnxn);
		logStats.setDuration(LogConstants.MINUTE);
		try {
			logStats.persistenceCassandra();
			collector.emit(new Values(entry));
			collector.ack(input);
		} catch (ConnectionException e) {
			collector.fail(input);
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(LogConstants.MINUTE_LOG_ENTRY));
	}

}
