package ca.dealsaccess.holt.jobs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.KafkaSpout;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.AbstractStormJob;
import ca.dealsaccess.holt.common.KafkaConfig;
import ca.dealsaccess.holt.kafka.KafkaSpoutFactory;
import ca.dealsaccess.holt.storm.bolt.PrinterBolt;

public final class PrinterJob extends AbstractStormJob {

	private static final Logger LOG = LoggerFactory.getLogger(PrinterJob.class);

	private boolean isLocal = true;

	public static void main(String[] args) {
		
		PrinterJob main = new PrinterJob();
		try {
			main.run(args);
		} catch (ConfigException e) {
			LOG.error("Unexpected ConfigException, exiting abnormally,", e);
			System.exit(2);
		} catch (IOException e) {
			LOG.error("Unexpected IOException, exiting abnormally,", e);
			System.exit(1);
		} catch (Exception e) {
			LOG.error("Unexpected Exception, exiting abnormally,", e);
			System.exit(1);
		}
		LOG.info("Exiting normally");
		System.exit(0);
	}

	private void run(String[] args) throws ConfigException, AlreadyAliveException, InvalidTopologyException,
			InterruptedException, IOException {
		LOG.info("Program start on "+this.getClass().getName());
		addOptions();
		if (parseArguments(args) == null) {
			return;
		}

		Config conf = new Config();
		conf.setDebug(true);

		StormTopology stormTopology = buildTopology();
		
		if (args != null && args.length > 0) {
			isLocal = false;
		}
		isLocal = true;
		if (!isLocal) {
			conf.setNumWorkers(4);
			StormSubmitter.submitTopology(args[0], conf, stormTopology);
		} else {
			conf.setNumWorkers(4);
			conf.setMaxTaskParallelism(4);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("counter-local", conf, stormTopology);
			
			while (true) {

			}

			// cluster.shutdown();
		}
	}

	private void addOptions() {
		addOption("topic", "t", "the input kafka topic", true);
	}

	private StormTopology buildTopology() throws ConfigException, IOException {

		KafkaSpout kafkaSpout = buildKafkaSpout();

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", kafkaSpout, 2);
		builder.setBolt("counter", new PrinterBolt(), 1).shuffleGrouping("kafkaSpout");
		// builder.setBolt("split", new SplitSentence(),
		// 8).shuffleGrouping("kafkaSpout");
		// builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split",
		// new Fields("word"));

		return builder.createTopology();
	}

	private KafkaSpout buildKafkaSpout() throws ConfigException, IOException {

		KafkaConfig kafkaConfig = new KafkaConfig(true);
		kafkaConfig.parseProperties();
		kafkaConfig.setKafkaTopic(getOption("topic"));

		return KafkaSpoutFactory.createKafkaSpout(kafkaConfig);
	}

}
