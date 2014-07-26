package ca.dealsaccess.holt.log;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;

import org.apache.commons.io.FilenameUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.log.ip.HttpIPResolver;

//example: 157.55.39.81 - - [14/Jul/2014:22:23:29 -0400] "GET /index.php?home=deals&act=index&groupsite=Reitmans&cateid_1=98&cateid_2=100&price=$0-$10 HTTP/1.1" 200 57460 "-" "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)"
public class ApacheLogEntry extends AbstractLogEntry {
	
	private static final Logger LOG = LoggerFactory.getLogger(ApacheLogEntry.class);
	
	private static final String CONTRY_KEY = "country_name";
	
	private static final String CITY_KEY = "city";
	
	public ApacheLogEntry() {
		
	}
	
	public ApacheLogEntry(String logText) {
		super(logText);
	}

	@Override
	public void parseLogText() {
		try {
			String tmpStr = logText;
			tmpStr = parseDate(tmpStr);
			tmpStr = parseUrl(tmpStr);
			parseOthers(tmpStr);
		} catch (LogEntryException e) {
			LOG.warn(e.getMessage());
		} catch (ParseException e) {
			LOG.warn(e.getClass().getName()+": "+e.getMessage());
		} catch (Exception e) {
			LOG.warn(e.getClass().getName()+": "+e.getMessage());
		}
	}
	
	private void parseOthers(String tmpStr) throws LogEntryException {
		String[] tmpArr = whiteSpace.split(tmpStr.replace("-", ""));
		if(tmpArr.length >= 3) {
			ip = tmpArr[0];
			findLocationbyIP();
			statusCode = Integer.parseInt(tmpArr[1]);
			responseSize = Integer.parseInt(tmpArr[2]);
		} else {
			throw new LogEntryException(this.getClass().getName()+": parse IP/statusCode/responseSize failed for apache log");
		}
		
	}

	private void findLocationbyIP() {
		//find country city from url
		JSONObject json = HttpIPResolver.resolveIP(ip);
		country = (String) json.get(CONTRY_KEY);
		city = (String) json.get(CITY_KEY);
	}

	//example: GET /index.php?home=deals&act=index&groupsite=Reitmans&cateid_1=98&cateid_2=100&price=$0-$10 HTTP/1.1
	private String parseUrl(String tmpStr) throws LogEntryException, IndexOutOfBoundsException {
		
		Matcher dateMatcher = urlPattern.matcher(tmpStr);
		if(dateMatcher.find()) {
			method = dateMatcher.group(1);
			url = dateMatcher.group(2);
			protocol = dateMatcher.group(3);
			extension = FilenameUtils.getExtension(url);
			
			return tmpStr.replaceFirst(urlPattern.toString(), "");
		} else {
			throw new LogEntryException(this.getClass().getName()+": urlPattern cannot match for apache log");
		}
	}

	//example: 14/Jul/2014:22:23:29 -0400
	private String parseDate(String tmpStr) throws ParseException, LogEntryException {
		
		Matcher dateMatcher = datePattern.matcher(tmpStr);
		if(dateMatcher.find()) {
			String dateString = dateMatcher.group(1);
			DateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z");
			date = dateFormat.parse(dateString);
			timestamp = date.getTime();
			timezone = whiteSpace.split(dateString)[1];
			
			return tmpStr.replaceFirst(datePattern.toString(), "");
		} else {
			throw new LogEntryException(this.getClass().getName()+": datePattern cannot match for apache log");
		}
	}

	
	
	
	
}

	
