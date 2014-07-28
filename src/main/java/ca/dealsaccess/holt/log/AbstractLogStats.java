package ca.dealsaccess.holt.log;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public abstract class AbstractLogStats {

	protected static final String FIELD_COLUMN_ALL_VALUE = "ALL";
	
	protected AbstractLogEntry entry;
	
	protected OutputCollector collector;
	
	protected Tuple tuple;
	
	protected AstyanaxCnxn astyanaxCnxn;
	
	protected Keyspace keyspace;
	
	protected static final long ONE = 1L;
	
	protected static final long ZERO = 0L;
	
	
	
	protected String rowKeyValue = null;
	
	protected String duration = null;
	
	protected boolean existed = false;
	
	public AbstractLogStats(AbstractLogEntry entry, OutputCollector collector, Tuple tuple, AstyanaxCnxn astyanaxCnxn) {
		this.entry = entry;
		this.collector = collector;
		this.tuple = tuple;
		this.astyanaxCnxn = astyanaxCnxn;
		this.keyspace = astyanaxCnxn.getKeyspace();
		this.existed = entry.isExisted();
	}


	public AbstractLogStats(ApacheLogEntry entry, OutputCollector collector, Tuple tuple) {
		this.entry = entry;
		this.collector = collector;
		this.tuple = tuple;
	}


	public AbstractLogStats setDuration(String duration) {
		this.duration = duration;
		return this;
	}

	
	public String getRowKeyValue() {
		String value = null;
		if(duration.equals(LogConstants.MINUTE)) {
			value = entry.getMinuteForTime().toString();
		} else if(duration.equals(LogConstants.HOUR)) {
			value = entry.getHourForTime().toString();
		} else if(duration.equals(LogConstants.DAY)) {
			value = entry.getDayForTime().toString();
		} else if(duration.equals(LogConstants.WEEK)) {
			value = entry.getWeekForTime().toString();
		} else if(duration.equals(LogConstants.MONTH)) {
			value = entry.getMonthForTime().toString();
		} else if(duration.equals(LogConstants.YEAR)) {
			value = entry.getYearForTime().toString();
		}
		return value;
	}


	public void emitLogEntry() {
		collector.emit(new Values(entry));
		collector.ack(tuple);
	}


	

	public abstract void persistenceCassandra() throws ConnectionException;


	

}
