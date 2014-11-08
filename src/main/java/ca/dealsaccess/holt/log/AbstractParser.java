package ca.dealsaccess.holt.log;


import java.util.Date;

import org.apache.commons.lang.StringUtils;


public abstract class AbstractParser {
	
	protected String text;
	
	
	protected String ip = StringUtils.EMPTY;
	protected int statusCode = 0;
	protected int responseSize = 0;
	protected String method = StringUtils.EMPTY;
	protected String url = StringUtils.EMPTY;
	protected String protocol = StringUtils.EMPTY;
	protected Date date;
	protected long timestamp = 0L;
	protected String timezone = StringUtils.EMPTY;
	
	
	
	
	
	
	
	public AbstractParser(String text) {
		this.text = text;
	}
	
	public abstract void parse();

	public String getText() {
		return text;
	}

	public String getIp() {
		return ip;
	}

	public int getStatusCode() {
		return statusCode;
	}

	public int getResponseSize() {
		return responseSize;
	}

	public String getMethod() {
		return method;
	}

	public String getUrl() {
		return url;
	}

	public String getProtocol() {
		return protocol;
	}

	public Date getDate() {
		return date;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public String getTimezone() {
		return timezone;
	}
	
	
	
	

}
