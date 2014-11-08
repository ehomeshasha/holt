package ca.dealsaccess.holt.log;

import java.util.Date;
import java.util.TimeZone;


import net.sf.json.JSONObject;

public class JsonParser extends AbstractParser {

	public JsonParser(String text) {
		super(text);
	}

	@Override
	public void parse() {
		
		JSONObject jb = JSONObject.fromObject(text);
		
		ip = jb.getString("REMOTE_ADDR");
		statusCode = 200;
		responseSize = 0;
		method = jb.getString("REQUEST_METHOD");
		url = jb.getString("REQUEST_URI");
		protocol = jb.getString("SERVER_PROTOCOL");
		timestamp = Long.parseLong(jb.getString("REQUEST_TIME"))*1000;
		date = new Date(timestamp);
		timezone = TimeZone.getDefault().getID();
		
		
		
		
		
		
		
		
		
		
		
		
		
	}

	
	
	
	
}
