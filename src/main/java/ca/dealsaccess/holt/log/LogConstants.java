package ca.dealsaccess.holt.log;



public class LogConstants {
	
	public static final String LOG_TEXT = "LogText";
	
	public static final String LOG_ENTRY = "LogEntry";
	
    public static final String LOG_INDEX_ID = "LogIndexId";
    
    public static final String LOG_TIMESTAMP = "Timestamp";
    
    public static final String LOG_COUNT = "Count";

	public static final String LOG_ENTRY_INDEXER = "LogEntryIndexer";
	
	
	
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
	
	private static final String FIELD_COLUMN_IP_COUNTRY = "IPCountry";

	private static final String FIELD_COLUMN_IP_CITY = "IPCity";
	
	public static final String[] LOG_DURATION = { MINUTE, HOUR, DAY, WEEK, MONTH, YEAR };

	public static final String[] LOG_COLUMNS = { 
		FIELD_COLUMN_ALL, 
		FIELD_COLUMN_IP,
		FIELD_COLUMN_URL, 
		FIELD_COLUMN_METHOD,
		FIELD_COLUMN_PROTOCOL, 
		FIELD_COLUMN_EXT,
		FIELD_COLUMN_STATUSCODE, 
		FIELD_COLUMN_RESPONSESIZE,
		FIELD_COLUMN_COUNTRY, 
		FIELD_COLUMN_CITY,
		FIELD_COLUMN_IP_COUNTRY,
		FIELD_COLUMN_IP_CITY,
	};
	
	public static final String[] LOG_EXISTS_COLUMNS = {
		FIELD_COLUMN_IP_COUNTRY,
		FIELD_COLUMN_IP_CITY
	};
	

	public static final String MINUTE_LOG_ENTRY = "MinuteLogEntry";
	
	public static final String HOUR_LOG_ENTRY = "MinuteLogEntry";
	
	public static final String DAY_LOG_ENTRY = "DayLogEntry";
	
	public static final String WEEK_LOG_ENTRY = "WeekLogEntry";
	
	public static final String MONTH_LOG_ENTRY = "MonthLogEntry";
	
	public static final String YEAR_LOG_ENTRY = "YearLogEntry";
	
	
	
	
	
	
	

}
