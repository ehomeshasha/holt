/*package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.astyanax.LogStatsColumnFamily;
import ca.dealsaccess.holt.log.AbstractLogStats;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.ApacheLogStats;
import ca.dealsaccess.holt.log.LogConstants;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class LogStatsBolt extends BaseRichBolt {

	public static Logger LOG = LoggerFactory.getLogger(LogStatsBolt.class);
	
	private OutputCollector collector;
	
	private AstyanaxCnxn astyanaxCnxn = null;
	
	private Keyspace keyspace = null;
	
	private ColumnFamily<String, String> IPCF = null;
	
	private final String duration;
	
	public LogStatsBolt(String duration) {
		this.duration = duration;
	}
	
	
	
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;

		try {
			astyanaxCnxn = new AstyanaxCnxn();
			astyanaxCnxn.connect();
			keyspace = astyanaxCnxn.getKeyspace();
			IPCF = astyanaxCnxn.getCFbyName(LogStatsColumnFamily.IP_TABLE+"_"+duration);
		} catch (Exception e) {
			LOG.error("cannot connect to cassandra");
		}
	}

	@Override
	public void execute(Tuple input) {

		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		AbstractLogStats logStats = new ApacheLogStats(entry, collector, input, astyanaxCnxn);
		logStats.setDuration(duration);
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
				collector.fail(input);
				return;
			}
		}
		
		
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
		declarer.declare(new Fields(duration+LogConstants.LOG_ENTRY));
	}

}
*/