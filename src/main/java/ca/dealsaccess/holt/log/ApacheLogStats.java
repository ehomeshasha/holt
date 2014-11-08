/*package ca.dealsaccess.holt.log;

import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;





import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;

import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.astyanax.LogStatsColumnFamily;
import ca.dealsaccess.holt.util.GsonUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;

public class ApacheLogStats extends AbstractLogStats {

	private static Logger LOG = LoggerFactory.getLogger(ApacheLogStats.class);
	
	private StringBuilder sb = new StringBuilder();
	
	public ApacheLogStats(ApacheLogEntry apacheLogEntry, OutputCollector collector, Tuple tuple, AstyanaxCnxn astyanaxCnxn) {
		super(apacheLogEntry, collector, tuple, astyanaxCnxn);
	}

	public ApacheLogStats(ApacheLogEntry apacheLogEntry, OutputCollector collector, Tuple tuple) {
		super(apacheLogEntry, collector, tuple);
	}

	@Override
	public void persistenceCassandra() throws ConnectionException {
		
		rowKeyValue = getRowKeyValue();
		
		MutationBatch mutation = keyspace.prepareMutationBatch();
		
		String[] columnArray = ((ApacheLogEntry) entry).getIPNoneExistsArray();
		
		for(int i=0;i<LogConstants.LOG_COLUMNS.length;i++) {
			boolean addToCassandra = ! (ArrayUtils.contains(LogConstants.LOG_EXISTS_COLUMNS, LogConstants.LOG_COLUMNS[i]));
			String cfName = sb.append(LogStatsColumnFamily.LOGGING)
					.append(duration).append(LogConstants.LOG_COLUMNS[i]).append(LogStatsColumnFamily.COUNTER).toString();
			ColumnFamily<String, String> cf = astyanaxCnxn.getCFbyName(cfName);
			
			if(!entry.isExisted() || addToCassandra) {
				mutation.withRow(cf, rowKeyValue).incrementCounterColumn(columnArray[i], ONE);
				Map<String, String> testMap = ImmutableMap.<String, String>builder()
				        .put("cfName", cfName)
				        .put("rowKeyValue", rowKeyValue)
				        .put("column", columnArray[i])
				        .put("IncrementAmount", String.valueOf(ONE))
				        .build();
				LOG.info("Saving logStats to Cassandra: "+GsonUtils.toJson(testMap));
			}
			sb.setLength(0);
		}
		mutation.execute();
		
	}
}
*/