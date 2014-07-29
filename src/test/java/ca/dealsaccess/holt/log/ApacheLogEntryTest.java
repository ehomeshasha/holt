package ca.dealsaccess.holt.log;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FilenameUtils;
import org.junit.Test;

import com.google.gson.Gson;






public class ApacheLogEntryTest {
	
	//private static final String logText = "157.55.39.81 - - [14/Jul/2014:22:23:29 -0400] \"GET /index.php?home=deals&act=index&groupsite=Reitmans&cateid_1=98&cateid_2=100&price=$0-$10 HTTP/1.1\" 200 57460 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"";
	private static final String logText = "175.42.85.236 - - [14/Jul/2014:22:23:43 -0400] \"GET /index.php?home=index&act=jump&id=2_325400/index.php HTTP/1.1\" 302 1 \"-\" \"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1;)\"";
	
	@Test
	public void parseDateTest() {
		
		ApacheLogEntry logEntry = new ApacheLogEntry(logText);
		System.out.println("logText: "+logText);
		logEntry.parseLogText();
		Gson gson = new Gson();
		System.out.printf("%s: %s", "parsedLogEntry", gson.toJson(logEntry));
		System.out.println();
		
	}
	
	Pattern pattern = Pattern.compile("^([0-9a-zA-Z]+)");
	
	@Test
	public void getExtension() {
		String url = "/index.php?home=index&act=jump&id=2_325400";
		String ext = FilenameUtils.getExtension(url);
		
		Matcher matcher = pattern.matcher(ext);
		matcher.find();
		String result = matcher.group(1);
		
		System.out.println(result);
	}
	
}
