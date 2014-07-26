package ca.dealsaccess.holt.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Values;

public class ApacheLogStats extends AbstractLogStats {

	public ApacheLogStats(ApacheLogEntry apacheLogEntry, OutputCollector collector) {
		super(apacheLogEntry, collector);
	}
	
	public void emit() {
		rowKeyValue = getRowKeyValue();
		collector.emit(new Values(
				rowKeyValue, 
				ONE,
				//FIELD_COLUMN_ALL_VALUE,
				entry.getIp(),
				//entry.getUrl(),
				//entry.getMethod(),
				//entry.getProtocol(),
				entry.getExtension()
				//entry.getStatusCode(),
				//entry.getResponseSize(),
				//entry.getCountry(),
				//entry.getCity()
		));
	}

}
