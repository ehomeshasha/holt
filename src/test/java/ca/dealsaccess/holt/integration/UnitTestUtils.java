package ca.dealsaccess.holt.integration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class UnitTestUtils {

	public static String readFile(String file) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(
				IntegrationTopologyTest.class.getResourceAsStream(file)));
		String line = null;
		StringBuilder stringBuilder = new StringBuilder();
		String ls = System.getProperty("line.separator");

		while ((line = reader.readLine()) != null) {
			stringBuilder.append(line);
			stringBuilder.append(ls);
		}

		return stringBuilder.toString();
	}

	
}
