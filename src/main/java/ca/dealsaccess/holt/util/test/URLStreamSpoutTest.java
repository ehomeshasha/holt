package ca.dealsaccess.holt.util.test;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import ca.dealsaccess.holt.log.LogConstants;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class URLStreamSpoutTest {
	public static FixedBatchSpout createFixedBatchSpout() throws IOException {
		//InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("test.log");
		//BufferedReader br = new BufferedReader(new InputStreamReader(in));
		BufferedReader br = new BufferedReader(new FileReader("test.log"));
		String line = null;
		List<Values> outputs = new ArrayList<Values>();
		while((line = br.readLine()) != null) {
			outputs.add(new Values(line));
		}
		br.close();
		FixedBatchSpout spout = new FixedBatchSpout(new Fields(LogConstants.LOG_TEXT), 3, outputs.toArray(new Values[0]));
		spout.setCycle(true);
		return spout;
	}

}
