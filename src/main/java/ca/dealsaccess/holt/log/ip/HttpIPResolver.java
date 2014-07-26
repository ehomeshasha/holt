package ca.dealsaccess.holt.log.ip;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

public class HttpIPResolver {

	public static final String url = "http://api.hostip.info/get_json.php";

	public static JSONObject resolveIP(String ip) {
		URL geoUrl = null;
		BufferedReader in = null;
		try {
			geoUrl = new URL(url + "?ip=" + ip);
			URLConnection connection = geoUrl.openConnection();
			in = new BufferedReader(new InputStreamReader(connection.getInputStream()));

			JSONObject json = (JSONObject) JSONValue.parse(in);
			in.close();
			return json;
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}
		return null;
	}
}
