package ca.dealsaccess.holt.trident;

import java.io.IOException;

import org.junit.Test;

import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.util.test.URLStreamSpoutTest;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class URLTridentTopologyTest {
	
	
	
	
	
	
	
	@Test
	public void runWordSplitTest() throws IOException, InterruptedException {
		
		FixedBatchSpout spout = URLStreamSpoutTest.createFixedBatchSpout();
		
		TridentTopology topology = new TridentTopology();
		/*
		TridentState wordCounts = topology.newStream("spout1", spout)
				.each(new Fields(LogConstants.LOG_TEXT), new Split(), new Fields("word"))
				.groupBy(new Fields("word"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
				.parallelismHint(6);
		*/
		
		topology.newStream("spout1", spout)
		.each(new Fields(LogConstants.LOG_TEXT), new Split(), new Fields("word"))
		.groupBy(new Fields("word"));
		
		
		StormTopology stormTopology = topology.build();
		
		Config conf = new Config();
		conf.setDebug(true);
		
		conf.setNumWorkers(1);
		conf.setMaxTaskParallelism(1);

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("urltrident-local", conf, stormTopology);
		
		synchronized(this) {
			this.wait();
		}
		
	}
	
	
}
