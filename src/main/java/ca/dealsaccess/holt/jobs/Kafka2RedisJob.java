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
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaSpout;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentTopology;
import storm.trident.testing.Split;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.AbstractStormJob;
import ca.dealsaccess.holt.common.KafkaConfig;
import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.kafka.KafkaSpoutFactory;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.redis.RedisUtils;
import ca.dealsaccess.holt.storm.bolt.LogRedisBolt;
import ca.dealsaccess.holt.trident.function.LogRedisFunction;
import ca.dealsaccess.holt.util.test.URLStreamSpoutTest;

public final class Kafka2RedisJob extends AbstractStormJob {

	private static final Logger LOG = LoggerFactory.getLogger(Kafka2RedisJob.class);

	private boolean isLocal = true;
	
	private Config conf = new Config();
	
	private TopologyBuilder builder;
	
	private StormTopology topology;
	
	private TridentTopology tridentTopology;
	
	public static void main(String[] args) {
		
		Kafka2RedisJob main = new Kafka2RedisJob();
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

		conf.setDebug(true);
		
		conf.put(RedisConstants.REDIS_KEY, getOption("topic"));
		RedisUtils.configure(conf);
		
		
		
		if(!hasOption("trident")) {
			buildTopology();
		} else {
			buildTridentTopology();
		}
		
		if (args != null && args.length > 0) {
			isLocal = false;
		}
		isLocal = true;
		if (!isLocal) {
			conf.setNumWorkers(1);
			StormSubmitter.submitTopology(args[0], conf, topology);
		} else {
			conf.setNumWorkers(1);
			conf.setMaxTaskParallelism(1);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("logredis-local", conf, topology);
			
			synchronized(this) {
				this.wait();
			}
			/*
			while (true) {

			}*/

			// cluster.shutdown();
		}
	}

	

	private void addOptions() {
		addOption("topic", "t", "the input kafka topic", true);
		addFlag("trident", "tr", "whether using trident topology");
	}

	private void createBuilder() throws ConfigException, IOException {
		KafkaSpout kafkaSpout = buildKafkaSpout();

		builder = new TopologyBuilder();
		builder.setSpout("kafkaSpout", kafkaSpout, 2);
		builder.setBolt("logRedisSpout", new LogRedisBolt(), 1).shuffleGrouping("kafkaSpout");
	}
	
	private void buildTopology() throws ConfigException, IOException {
		createBuilder();
		topology = builder.createTopology(); 
	}

	private void createTridentTopology() throws ConfigException, IOException {
		TransactionalTridentKafkaSpout tridentKafkaSpout = buildTridentKafkaSpout();
		
		tridentTopology = new TridentTopology();
		tridentTopology.newStream("kafkaSpout", tridentKafkaSpout).shuffle()
		
		//tridentTopology.newStream("urlSpout", URLStreamSpoutTest.createFixedBatchSpout())
		
		.each(new Fields(StringScheme.STRING_SCHEME_KEY), 
				new LogRedisFunction(
						(String) conf.get(RedisConstants.REDIS_HOST), 
						Integer.parseInt((String) conf.get(RedisConstants.REDIS_PORT)), 
						getOption("topic")),
				new Fields(LogConstants.LOG_TEXT));
		
		//.each(new Fields(StringScheme.STRING_SCHEME_KEY), 
		//.each(new Fields(LogConstants.LOG_TEXT),
		//		new Split(),
		//		new Fields("words"))
		//.groupBy(new Fields("words"));
		
	}
	
	

	private void buildTridentTopology() throws ConfigException, IOException {
		createTridentTopology();
		topology = tridentTopology.build();
	}
	
	
	
	private KafkaSpout buildKafkaSpout() throws ConfigException, IOException {

		KafkaConfig kafkaConfig = new KafkaConfig(true);
		kafkaConfig.parseProperties();
		kafkaConfig.setKafkaTopic(getOption("topic"));

		return KafkaSpoutFactory.createKafkaSpout(kafkaConfig);
	}

	private TransactionalTridentKafkaSpout buildTridentKafkaSpout() throws ConfigException, IOException {
		
		KafkaConfig kafkaConfig = new KafkaConfig(true);
		kafkaConfig.parseProperties();
		kafkaConfig.setKafkaTopic(getOption("topic"));
		
		
		return KafkaSpoutFactory.createTransactionalTridentKafkaSpout(kafkaConfig);
	}
	
	
}
