package ca.dealsaccess.holt.trident.function;

import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("serial")
public class LogRulesFunction extends BaseFunction {
    
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		
		String logText = (String) tuple.getValueByField(LogConstants.LOG_TEXT);
		ApacheLogEntry logEntry = new ApacheLogEntry(logText);
		logEntry.parseLogText();
		collector.emit(new Values(logEntry));
		
    }
}
