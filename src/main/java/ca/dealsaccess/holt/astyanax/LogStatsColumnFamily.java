package ca.dealsaccess.holt.astyanax;

import ca.dealsaccess.holt.log.AbstractLogStats;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class LogStatsColumnFamily {

	public static final String LOGGING = "Logging";
	
	public static final String COUNTER = "Counter";
	
	private final AstyanaxCnxn astyanaxCnxn;

	private final Keyspace keyspace;

	public static StringBuilder sb = new StringBuilder();

	public String[] ColumnFamilyNames;

	public static final String[] duration = { "Minute", "Hour", "Day", "Week", "Month", "Year" };

	public static final String[] columns = { AbstractLogStats.FIELD_COLUMN_ALL, AbstractLogStats.FIELD_COLUMN_IP,
			AbstractLogStats.FIELD_COLUMN_URL, AbstractLogStats.FIELD_COLUMN_METHOD,
			AbstractLogStats.FIELD_COLUMN_PROTOCOL, AbstractLogStats.FIELD_COLUMN_EXT,
			AbstractLogStats.FIELD_COLUMN_STATUSCODE, AbstractLogStats.FIELD_COLUMN_RESPONSESIZE,
			AbstractLogStats.FIELD_COLUMN_COUNTRY, AbstractLogStats.FIELD_COLUMN_CITY };
	/*
	//MINUTE
	public static final String FIELD_COLUMN_MINUTE_ALL = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_ALL).toString();
	
	public static final String FIELD_COLUMN_MINUTE_IP = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_IP).toString();
	
	public static final String FIELD_COLUMN_MINUTE_URL = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_URL).toString();
	
	public static final String FIELD_COLUMN_MINUTE_METHOD = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_METHOD).toString();
	
	public static final String FIELD_COLUMN_MINUTE_PROTOCOL = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_PROTOCOL).toString();
	
	public static final String FIELD_COLUMN_MINUTE_EXT = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_EXT).toString();
	
	public static final String FIELD_COLUMN_MINUTE_STATUSCODE = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_STATUSCODE).toString();
	
	public static final String FIELD_COLUMN_MINUTE_RESPONSESIZE = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_RESPONSESIZE).toString();
	
	public static final String FIELD_COLUMN_MINUTE_COUNTRY = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_COUNTRY).toString();
	
	public static final String FIELD_COLUMN_MINUTE_CITY = new StringBuilder().append(LOGGING).append(AbstractLogStats.MINUTE)
			.append(AbstractLogStats.FIELD_COLUMN_CITY).toString();
	
	//HOUR
	public static final String FIELD_COLUMN_HOUR_ALL = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_ALL).toString();
	
	public static final String FIELD_COLUMN_HOUR_IP = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_IP).toString();
	
	public static final String FIELD_COLUMN_HOUR_URL = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_URL).toString();
	
	public static final String FIELD_COLUMN_HOUR_METHOD = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_METHOD).toString();
	
	public static final String FIELD_COLUMN_HOUR_PROTOCOL = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_PROTOCOL).toString();
	
	public static final String FIELD_COLUMN_HOUR_EXT = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_EXT).toString();
	
	public static final String FIELD_COLUMN_HOUR_STATUSCODE = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_STATUSCODE).toString();
	
	public static final String FIELD_COLUMN_HOUR_RESPONSESIZE = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_RESPONSESIZE).toString();
	
	public static final String FIELD_COLUMN_HOUR_COUNTRY = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_COUNTRY).toString();
	
	public static final String FIELD_COLUMN_HOUR_CITY = new StringBuilder().append(LOGGING).append(AbstractLogStats.HOUR)
			.append(AbstractLogStats.FIELD_COLUMN_CITY).toString();
	
	//DAY
	public static final String FIELD_COLUMN_DAY_ALL = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_ALL).toString();
	
	public static final String FIELD_COLUMN_DAY_IP = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_IP).toString();
	
	public static final String FIELD_COLUMN_DAY_URL = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_URL).toString();
	
	public static final String FIELD_COLUMN_DAY_METHOD = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_METHOD).toString();
	
	public static final String FIELD_COLUMN_DAY_PROTOCOL = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_PROTOCOL).toString();
	
	public static final String FIELD_COLUMN_DAY_EXT = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_EXT).toString();
	
	public static final String FIELD_COLUMN_DAY_STATUSCODE = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_STATUSCODE).toString();
	
	public static final String FIELD_COLUMN_DAY_RESPONSESIZE = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_RESPONSESIZE).toString();
	
	public static final String FIELD_COLUMN_DAY_COUNTRY = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_COUNTRY).toString();
	
	public static final String FIELD_COLUMN_DAY_CITY = new StringBuilder().append(LOGGING).append(AbstractLogStats.DAY)
			.append(AbstractLogStats.FIELD_COLUMN_CITY).toString();
	
	//WEEK
	public static final String FIELD_COLUMN_WEEK_ALL = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_ALL).toString();
	
	public static final String FIELD_COLUMN_WEEK_IP = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_IP).toString();
	
	public static final String FIELD_COLUMN_WEEK_URL = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_URL).toString();
	
	public static final String FIELD_COLUMN_WEEK_METHOD = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_METHOD).toString();
	
	public static final String FIELD_COLUMN_WEEK_PROTOCOL = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_PROTOCOL).toString();
	
	public static final String FIELD_COLUMN_WEEK_EXT = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_EXT).toString();
	
	public static final String FIELD_COLUMN_WEEK_STATUSCODE = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_STATUSCODE).toString();
	
	public static final String FIELD_COLUMN_WEEK_RESPONSESIZE = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_RESPONSESIZE).toString();
	
	public static final String FIELD_COLUMN_WEEK_COUNTRY = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_COUNTRY).toString();
	
	public static final String FIELD_COLUMN_WEEK_CITY = new StringBuilder().append(LOGGING).append(AbstractLogStats.WEEK)
			.append(AbstractLogStats.FIELD_COLUMN_CITY).toString();
	
	//MONTH
	public static final String FIELD_COLUMN_MONTH_ALL = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_ALL).toString();
	
	public static final String FIELD_COLUMN_MONTH_IP = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_IP).toString();
	
	public static final String FIELD_COLUMN_MONTH_URL = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_URL).toString();
	
	public static final String FIELD_COLUMN_MONTH_METHOD = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_METHOD).toString();
	
	public static final String FIELD_COLUMN_MONTH_PROTOCOL = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_PROTOCOL).toString();
	
	public static final String FIELD_COLUMN_MONTH_EXT = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_EXT).toString();
	
	public static final String FIELD_COLUMN_MONTH_STATUSCODE = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_STATUSCODE).toString();
	
	public static final String FIELD_COLUMN_MONTH_RESPONSESIZE = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_RESPONSESIZE).toString();
	
	public static final String FIELD_COLUMN_MONTH_COUNTRY = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_COUNTRY).toString();
	
	public static final String FIELD_COLUMN_MONTH_CITY = new StringBuilder().append(LOGGING).append(AbstractLogStats.MONTH)
			.append(AbstractLogStats.FIELD_COLUMN_CITY).toString();
	
	//YEAR
	public static final String FIELD_COLUMN_YEAR_ALL = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_ALL).toString();
	
	public static final String FIELD_COLUMN_YEAR_IP = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_IP).toString();
	
	public static final String FIELD_COLUMN_YEAR_URL = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_URL).toString();
	
	public static final String FIELD_COLUMN_YEAR_METHOD = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_METHOD).toString();
	
	public static final String FIELD_COLUMN_YEAR_PROTOCOL = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_PROTOCOL).toString();
	
	public static final String FIELD_COLUMN_YEAR_EXT = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_EXT).toString();
	
	public static final String FIELD_COLUMN_YEAR_STATUSCODE = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_STATUSCODE).toString();
	
	public static final String FIELD_COLUMN_YEAR_RESPONSESIZE = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_RESPONSESIZE).toString();
	
	public static final String FIELD_COLUMN_YEAR_COUNTRY = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_COUNTRY).toString();
	
	public static final String FIELD_COLUMN_YEAR_CITY = new StringBuilder().append(LOGGING).append(AbstractLogStats.YEAR)
			.append(AbstractLogStats.FIELD_COLUMN_CITY).toString();
	*/
	
	public LogStatsColumnFamily(AstyanaxCnxn astyanaxCnxn) {
		this.astyanaxCnxn = astyanaxCnxn;
		this.keyspace = astyanaxCnxn.getKeyspace();
	}

	public void createAllCounterCFIfNotExist() throws ConnectionException {
		
		for(int i=0;i<duration.length;i++) {
			for(int j=0;j<columns.length;j++) {
				sb.setLength(0);
				String cfName = sb.append(LOGGING).append(duration[i]).append(columns[j]).append(COUNTER).toString();
				astyanaxCnxn.createCounterCFIfNotExists(cfName);
			}
		}
	}

}
