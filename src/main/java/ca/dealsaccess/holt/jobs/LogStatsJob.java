package ca.dealsaccess.holt.jobs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentTopology;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.AbstractStormJob;
import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.redis.RedisUtils;
import ca.dealsaccess.holt.storm.bolt.IndexerBolt;
import ca.dealsaccess.holt.storm.bolt.LogRulesBolt;
import ca.dealsaccess.holt.storm.bolt.VolumeCountingBolt;
import ca.dealsaccess.holt.storm.spout.RedisSpout;
import ca.dealsaccess.holt.trident.function.IndexerFunction;
import ca.dealsaccess.holt.trident.function.LogRulesFunction;

public final class LogStatsJob extends AbstractStormJob {

	private static final Logger LOG = LoggerFactory.getLogger(LogStatsJob.class);

	private static final String localTopologyName = "logstats-local";
	
	private static final String clusterTopologyName = "logstats-cluster";
	
	private boolean isLocal = true;
	
	private TopologyBuilder builder;
	
	private StormTopology topology;
	
	private Config conf = new Config();
	
	private LocalCluster cluster;

	public static void main(String[] args) {

		LogStatsJob main = new LogStatsJob();
		try {
			main.initializeAndRun(args);
		} catch (ConnectionException e) {
			LOG.error("Unexpected ConnectionException, exiting abnormally,", e);
			System.exit(2);
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

	private void initializeAndRun(String[] args) throws ConfigException, AlreadyAliveException, InvalidTopologyException,
			InterruptedException, IOException, ConnectionException {
		LOG.info("Program start on " + this.getClass().getName());
		initialize(args);
		run();
		
	}
	
	private void run() throws AlreadyAliveException, InvalidTopologyException {
		isLocal = true;
		if (!isLocal) {
			runCluster();
		} else {
			runLocal(Integer.MAX_VALUE);
		}
	}
	
	private void initialize(String[] args) throws ConfigException, IOException, ConnectionException {
		addOptions();
		if (parseArguments(args) == null) {
			return;
		}

		conf.setDebug(true);

		conf.put(RedisConstants.REDIS_KEY, getOption("redisKey"));
		RedisUtils.configure(conf);

		Config.registerSerialization(conf, ApacheLogEntry.class, FieldSerializer.class);

		
		if(!hasOption("trident")) {
			buildTopology();
		} else {
			buildTridentTopology();
		}
	}

	

	private void addOptions() {
		addOption("redisKey", "rk", "the key of redis list", true);
		addFlag("trident", "tr", "whether using trident topology");
	}

	private void createBuilder() throws ConnectionException, ConfigException {
		builder = new TopologyBuilder();

		// -o LOG_TEXT: {String}
		builder.setSpout("redisSpout", new RedisSpout(), 1);

		// -i [LOG_TEXT: {String}] -o [LOG_ENTRY: {ApacheLogEntry}]
		builder.setBolt("logRules", new LogRulesBolt(), 1).shuffleGrouping("redisSpout");

		// -i [LOG_ENTRY: {ApacheLogEntry}] -o [LOG_ENTRY: {ApacheLogEntry},
		// LOG_INDEX_ID: {String}]
		builder.setBolt("indexerBolt", new IndexerBolt(), 1).shuffleGrouping("logRules");

		// -i [LOG_ENTRY: {ApacheLogEntry}] -o [FIELD_ROW_KEY: {long},
		// FIELD_COLUMN: {ApacheLogEntry}, FIELD_INCREMENT: {1L}]
		builder.setBolt("volumeCountBolt", new VolumeCountingBolt(), 1).shuffleGrouping("logRules");

		builder.setBolt("cassandraCountBolt", buildCassandraBolt(), 1).shuffleGrouping("volumeCountBolt");
	}
	
	private void buildTopology() throws ConfigException, ConnectionException {
		createBuilder();
		topology = builder.createTopology();
	}

	private void buildTridentTopology() {
		TridentTopology tridentTopology = new TridentTopology();
		tridentTopology.newStream(LogConstants.LOG_TEXT, new RedisSpout())
		.each(new Fields(LogConstants.LOG_TEXT), new LogRulesFunction(), new Fields(LogConstants.LOG_ENTRY))
		.each(new Fields(LogConstants.LOG_ENTRY), new IndexerFunction(), new Fields(LogConstants.LOG_INDEX_ID));
	}
	
	
	@SuppressWarnings("rawtypes")
	private CassandraCounterBatchingBolt buildCassandraBolt() throws ConnectionException, ConfigException {
		
		AstyanaxCnxn astyanaxCnxn = new AstyanaxCnxn();
		astyanaxCnxn.connect();
		astyanaxCnxn.createKeyspaceIfNotExists();
		astyanaxCnxn.createCFIfNotExists(AstyanaxCnxn.CASSANDRA_MINUTES_COUNT_CF_NAME);
		
		HashMap<String, Object> clientConfig = new HashMap<String, Object>();
		clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "127.0.0.1:9160");
		clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String[] { astyanaxCnxn.getConfig().getKeySpace() }));
		conf.put(AstyanaxCnxn.CASSANDRA_CONFIG_KEY, clientConfig);
		

		CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(
				astyanaxCnxn.getConfig().getKeySpace(), AstyanaxCnxn.CASSANDRA_CONFIG_KEY,
				AstyanaxCnxn.CASSANDRA_MINUTES_COUNT_CF_NAME, "rowKey",
				"IncrementAmount");
		logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
		
		return logPersistenceBolt;
	}
	
	
	
	public void runLocal(int runTime) {
		
		conf.setDebug(true);
		cluster = new LocalCluster();
		
		conf.setNumWorkers(1);
		conf.setMaxTaskParallelism(1);
		cluster.submitTopology(localTopologyName, conf, topology);
		
		if(runTime == Integer.MAX_VALUE) {
			synchronized (this) {
				try {
					this.wait();
				} catch (InterruptedException e) {
					shutDownLocal();
				}
			}
		} else {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}

	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology(localTopologyName);
			cluster.shutdown();
		}
	}

	public void runCluster() throws AlreadyAliveException, InvalidTopologyException {
		conf.setNumWorkers(1);
		conf.setMaxTaskParallelism(1);
		StormSubmitter.submitTopology(clusterTopologyName, conf, topology);
	}

	public TopologyBuilder getBuilder() {
		return builder;
	}

	public Config getConf() {
		return conf;
	}

	
	
	
}
