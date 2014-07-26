package ca.dealsaccess.holt.log;

import backtype.storm.task.OutputCollector;

public abstract class AbstractLogStats {

	
	
	protected OutputCollector collector;
	
	protected static final long ONE = 1L;
	
	protected AbstractLogEntry entry;
	
	protected String rowKeyValue = null;
	
	protected static final String ColumnNameForAll = "ALL";
	
	protected boolean visited = true;
	
	protected boolean incremented = true;
	
	protected String duration = null;
	
	
	public static final String MINUTE = "Minute";
	
	public static final String HOUR = "Hour";
	
	public static final String DAY = "Day";
	
	public static final String WEEK = "Week";
	
	public static final String MONTH = "Month";
	
	public static final String YEAR = "Year";
	
	public static final String FIELD_ROW_KEY = "rowKey";
	
	public static final String FIELD_INCREMENT = "IncrementAmount";
	
	public static final String FIELD_COLUMN_ALL = "ALL";
	
	public static final String FIELD_COLUMN_IP = "IP";
	
	public static final String FIELD_COLUMN_URL = "Url";
	
	public static final String FIELD_COLUMN_METHOD = "Method";
	
	public static final String FIELD_COLUMN_PROTOCOL = "Protocol";
	
	public static final String FIELD_COLUMN_EXT = "Extension";
	
	public static final String FIELD_COLUMN_STATUSCODE = "StatusCode";
	
	public static final String FIELD_COLUMN_RESPONSESIZE = "ResponseSize";
	
	public static final String FIELD_COLUMN_COUNTRY = "Country";
	
	public static final String FIELD_COLUMN_CITY = "City";
	
	
	
	
	public static final String FIELD_COLUMN_PV = "PV";
	
	
	
	public static final String FIELD_COLUMN_IP_COUNTRY = "IP_COUNTRY";
	
	public static final String FIELD_COLUMN_IP_CITY = "IP_CITY";
	
	
	
	
	
	public static final String FIELD_COLUMN_IP_NUM = "ip_num";
	
	public static final String FIELD_COLUMN_ALL_VALUE = "All";
	
	
	
	
	
	public AbstractLogStats(ApacheLogEntry apacheLogEntry, OutputCollector collector) {
		entry = apacheLogEntry;
		this.collector = collector;
		
	}


	public AbstractLogStats setDuration(String duration) {
		this.duration = duration;
		return this;
	}

	


	public abstract void emit(); 


	public String getRowKeyValue() {
		String value = null;
		if(duration.equals(MINUTE)) {
			value = entry.getMinuteForTime().toString();
		} else if(duration.equals(HOUR)) {
			value = entry.getHourForTime().toString();
		} else if(duration.equals(DAY)) {
			value = entry.getDayForTime().toString();
		} else if(duration.equals(WEEK)) {
			value = entry.getWeekForTime().toString();
		} else if(duration.equals(MONTH)) {
			value = entry.getMonthForTime().toString();
		} else if(duration.equals(YEAR)) {
			value = entry.getYearForTime().toString();
		}
		return value;
	}


	public void setVisited(boolean visited) {
		this.visited = visited;
	}


	public void setIncrement(boolean incremented) {
		this.incremented = incremented; 
		
	}

}
