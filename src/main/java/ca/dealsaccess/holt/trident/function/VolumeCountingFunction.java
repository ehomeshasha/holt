package ca.dealsaccess.holt.trident.function;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;

@SuppressWarnings("serial")
public class VolumeCountingFunction extends BaseFunction {

	public static final String FIELD_ROW_KEY = "rowKey";
	
	public static final String FIELD_INCREMENT = "IncrementAmount";
	
	public static final String FIELD_COLUMN = "IncrementColumn";
	
	
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		ApacheLogEntry entry = (ApacheLogEntry) tuple.getValueByField(LogConstants.LOG_ENTRY);
		collector.emit(new Values(
				entry.getMinuteForTime(), 
				1L,
				entry.getUrl()
		));
	}
	
	
}
