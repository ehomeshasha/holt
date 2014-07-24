package ca.dealsaccess.holt.trident.function;

import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class LogRulesFunction extends BaseFunction {
    private static final long serialVersionUID = 444491620425331677L;

	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		
		String logText = (String) tuple.getValueByField(LogConstants.LOG_TEXT);
		ApacheLogEntry logEntry = new ApacheLogEntry(logText);
		logEntry.parseLogText();
		collector.emit(new Values(logEntry));
		
    }
}
