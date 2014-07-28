package ca.dealsaccess.holt.astyanax;

import ca.dealsaccess.holt.log.LogConstants;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class LogStatsColumnFamily {

	public static final String LOGGING = "Logging";
	
	public static final String COUNTER = "Counter";
	
	public static final String IP_TABLE = "IP_TABLE";
	
	private final AstyanaxCnxn astyanaxCnxn;

	private StringBuilder sb = new StringBuilder();

	public String[] ColumnFamilyNames;

	
	public LogStatsColumnFamily(AstyanaxCnxn astyanaxCnxn) {
		this.astyanaxCnxn = astyanaxCnxn;
	}

	public void createAllCounterCFIfNotExist() throws ConnectionException {
		
		for(int i=0;i<LogConstants.LOG_DURATION.length;i++) {
			for(int j=0;j<LogConstants.LOG_COLUMNS.length;j++) {
				String cfName = sb.append(LOGGING).append(LogConstants.LOG_DURATION[i]).append(LogConstants.LOG_COLUMNS[j]).append(COUNTER).toString();
				astyanaxCnxn.createCounterCFIfNotExists(cfName);
				sb.setLength(0);
			}
		}
	}

	public void createAllIPCFIfNotExist() throws ConnectionException {
		
		for(int i=0;i<LogConstants.LOG_DURATION.length;i++) {
			String cfName = sb.append(IP_TABLE).append("_").append(LogConstants.LOG_DURATION[i]).toString();
			astyanaxCnxn.createCFIfNotExists(cfName);
			sb.setLength(0);
		}
		
		
		
	}

}
