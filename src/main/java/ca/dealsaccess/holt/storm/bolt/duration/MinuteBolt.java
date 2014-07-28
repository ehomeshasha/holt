package ca.dealsaccess.holt.storm.bolt.duration;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import com.netflix.astyanax.model.ColumnFamily;

import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.astyanax.LogStatsColumnFamily;
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
public class MinuteBolt extends BaseRichBolt {

	public static Logger LOG = LoggerFactory.getLogger(MinuteBolt.class);
	
	private OutputCollector collector;
	
	private AstyanaxCnxn astyanaxCnxn = null;
	
	private Keyspace keyspace = null;
	
	private ColumnFamily<String, String> IPCF = null;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		try {
			astyanaxCnxn = new AstyanaxCnxn();
			astyanaxCnxn.connect();
			keyspace = astyanaxCnxn.getKeyspace();
			IPCF = astyanaxCnxn.getCFbyName(LogStatsColumnFamily.IP_TABLE+"_"+LogConstants.LOG_DURATION[0]);
		} catch (Exception e) {
			LOG.error("cannot connect to cassandra");
		}
	}

	@Override
	public void execute(Tuple input) {
		
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		AbstractLogStats logStats = new ApacheLogStats(entry, collector, input);
		logStats.setDuration(LogConstants.MINUTE);
		String rowKey = logStats.getRowKeyValue();
			
		try {
			keyspace.prepareQuery(IPCF)
			    .getKey(rowKey)
			    .getColumn(entry.getIp())
			    .execute().getResult();
		} catch (ConnectionException e) {
			try {
				keyspace.prepareColumnMutation(IPCF, rowKey, entry.getIp())
				.putValue("existed", null)
				.execute();
				entry.setExisted(false);
			} catch (ConnectionException e1) {
				LOG.warn("cannot create column "+entry.getIp());
			}
		}
		
		logStats.emitLogEntry();
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(LogConstants.MINUTE_LOG_ENTRY));
	}

}
