package ca.dealsaccess.holt.log;

import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;


public abstract class AbstractLogEntry {
	
	protected String EMPTY = "";
	
	protected String logText = EMPTY;
	
	protected String ip = EMPTY;
	
	protected long timestamp = 0L;
	
	protected Date date;
	
	protected String timezone = EMPTY;
	
	protected String url = EMPTY;
	
	protected String method = EMPTY;
	
	protected String protocol = EMPTY;
	
	protected String extension = EMPTY;
	
	protected int statusCode = 0;
	
	protected int responseSize = 0;
	
	protected String country = EMPTY;
	
	protected String city = EMPTY;
	
	protected static final Pattern whiteSpace = Pattern.compile("\\s+");
	
	protected static final Pattern slash = Pattern.compile("/");
	
	protected static final Pattern datePattern = Pattern.compile("\\[([^\\[\\]]+)\\]");
	
	protected static final Pattern urlPattern = Pattern.compile("\"([^\\s]+)\\s([^\\s]+)\\s([^\\s]+)\"");
	
	public AbstractLogEntry(String logText) {
		this.logText = logText;
	}
	
	public AbstractLogEntry() {
		
	}
	
	public abstract void parseLogText();
	
	private Calendar calendar = Calendar.getInstance();
	
	private Calendar getMinuteCalendar() {
		calendar.setTime(date);
		calendar.set(Calendar.SECOND,0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar;
	}
	
	private Calendar getHourCalendar() {
		getMinuteCalendar().set(Calendar.MINUTE, 0);
		return calendar;
	}
	
	private Calendar getDayCalendar() {
		getHourCalendar().set(Calendar.HOUR, 0);
		return calendar;
	}
	
	private Calendar getWeekCalendar() {
		getDayCalendar().set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		return calendar;
	}
	
	
	private Calendar getMonthCalendar() {
		getDayCalendar().set(Calendar.DAY_OF_MONTH, 1);
		return calendar;
	}
	
	private Calendar getYearCalendar() {
		getDayCalendar().set(Calendar.DAY_OF_YEAR, 1);
		return calendar;
	}
	
	public Long getMinuteForTime() {
		getMinuteCalendar();
		return calendar.getTimeInMillis();
	}

	public Long getHourForTime() {
		return getHourCalendar().getTimeInMillis();
	}
	
	public Long getDayForTime() {
		return getDayCalendar().getTimeInMillis();
	}
	
	public Long getWeekForTime() {
		return getWeekCalendar().getTimeInMillis();
	}
	
	public Long getMonthForTime() {
		return getMonthCalendar().getTimeInMillis();
	}
	
	public Long getYearForTime() {
		return getYearCalendar().getTimeInMillis();
	}
	
	
	
	
	protected class LogEntryException extends Exception {

		private static final long serialVersionUID = 3855842782324675793L;
		
		public LogEntryException() {
			super();
		}
		
		public LogEntryException(String msg) {
			super(msg);
		}
		
		public LogEntryException(String message, Throwable cause) {
	        super(message, cause);
	    }

	    public LogEntryException(Throwable cause) {
	        super(cause);
	    }
	}

	public String getLogText() {
		return logText;
	}

	public String getIp() {
		return ip;
	}

	public long getTimeStamp() {
		return timestamp;
	}

	public Date getDate() {
		return date;
	}

	public String getTimezone() {
		return timezone;
	}

	public String getUrl() {
		return url;
	}

	public String getMethod() {
		return method;
	}

	public String getProtocol() {
		return protocol;
	}

	public String getExtension() {
		return extension;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public int getResponseSize() {
		return responseSize;
	}

	public String getCountry() {
		return country;
	}

	public String getCity() {
		return city;
	}

	

}
