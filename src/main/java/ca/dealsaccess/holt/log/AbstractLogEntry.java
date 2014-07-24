package ca.dealsaccess.holt.log;

import java.util.Date;
import java.util.regex.Pattern;


public abstract class AbstractLogEntry {
	
	protected String logText;
	
	protected String ip;
	
	protected long timestamp;
	
	protected Date date;
	
	protected String timezone;
	
	protected String url;
	
	protected String method;
	
	protected String protocol;
	
	protected String extension;
	
	protected int statusCode;
	
	protected int responseSize;
	
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

}
