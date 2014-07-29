package ca.dealsaccess.holt.log;

import java.util.Calendar;
import java.util.Date;
import java.util.regex.Pattern;


public abstract class AbstractLogEntry {
	
	protected static final Pattern extensionPattern = Pattern.compile("^([0-9a-zA-Z]+)");
	
	protected static final String SLASH = "/";
	
	protected static final String EMPTY = "";
	
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
	
	protected boolean existed = true;
	
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
	
	public void setDayCalendar(Calendar calendar) {
		calendar.set(Calendar.SECOND,0);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.AM_PM, Calendar.AM);
		calendar.set(Calendar.HOUR, 0);
	}
	
	public Long getMinuteForTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.SECOND,0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTimeInMillis();
	}
	
	public Long getHourForTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.SECOND,0);
		calendar.set(Calendar.MILLISECOND, 0);
		calendar.set(Calendar.MINUTE, 0);
		return calendar.getTimeInMillis();
	}
	
	public Long getDayForTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		setDayCalendar(calendar);
		return calendar.getTimeInMillis();
	}
	
	public Long getWeekForTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		setDayCalendar(calendar);
		calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
		return calendar.getTimeInMillis();
	}
	
	public Long getMonthForTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		setDayCalendar(calendar);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		return calendar.getTimeInMillis();
	}
	
	public Long getYearForTime() {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		setDayCalendar(calendar);
		calendar.set(Calendar.DAY_OF_YEAR, 1);
		return calendar.getTimeInMillis();
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

	public String getResponseSizeLevel() {
		String level = null;
		if(responseSize < 1024) {
			level = "<1K";
		} else if(responseSize < 50*1024) {
			level = "<50K";
		} else if(responseSize < 100*1024) {
			level = "<100K";
		} else if(responseSize < 1024*1024) {
			level = "<1M";
		} else if(responseSize < 10*1024*1024) {
			level = "<10M";
		} else {
			level = ">=10M";
		}
		return level;
	}
	
	public String getCountry() {
		return country;
	}

	public String getCity() {
		return city;
	}

	public boolean isExisted() {
		return existed;
	}

	public void setExisted(boolean existed) {
		this.existed = existed;
	}

	

}
