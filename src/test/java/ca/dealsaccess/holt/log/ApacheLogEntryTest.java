package ca.dealsaccess.holt.log;

import org.junit.Test;

import com.google.gson.Gson;






public class ApacheLogEntryTest {
	
	private static final String logText = "157.55.39.81 - - [14/Jul/2014:22:23:29 -0400] \"GET /index.php?home=deals&act=index&groupsite=Reitmans&cateid_1=98&cateid_2=100&price=$0-$10 HTTP/1.1\" 200 57460 \"-\" \"Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)\"";
	
	@Test
	public void parseDateTest() {
		
		ApacheLogEntry logEntry = new ApacheLogEntry(logText);
		System.out.println("logText: "+logText);
		logEntry.parseLogText();
		Gson gson = new Gson();
		System.out.printf("%s: %s", "parsedLogEntry", gson.toJson(logEntry));
		System.out.println();
		
	}
	
}
