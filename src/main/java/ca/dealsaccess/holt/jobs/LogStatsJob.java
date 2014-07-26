package ca.dealsaccess.holt.jobs;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.Stream;
import storm.trident.TridentTopology;

import com.esotericsoftware.kryo.serializers.FieldSerializer;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleCounterMapper;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;

import com.netflix.astyanax.serializers.StringSerializer;

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
import ca.dealsaccess.holt.astyanax.LogStatsColumnFamily;

import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.AbstractStormJob;
import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;

import ca.dealsaccess.holt.redis.RedisUtils;
import ca.dealsaccess.holt.storm.spout.RedisFixedBatchSpout;
import ca.dealsaccess.holt.storm.spout.RedisLPOPFixedBatchSpout;
import ca.dealsaccess.holt.storm.bolt.IndexerBolt;
import ca.dealsaccess.holt.storm.bolt.LogRulesBolt;
import ca.dealsaccess.holt.storm.bolt.LogStatsMinuteBolt;
import ca.dealsaccess.holt.storm.bolt.VolumeCountingBolt;
import ca.dealsaccess.holt.storm.spout.RedisSpout;
import ca.dealsaccess.holt.trident.function.IndexerFunction;
import ca.dealsaccess.holt.trident.function.LogRulesFunction;
import ca.dealsaccess.holt.trident.function.VolumeCountingFunction;
import ca.dealsaccess.holt.trident.function.WriteToFileFunction;

public final class LogStatsJob extends AbstractStormJob {

	private static final Logger LOG = LoggerFactory.getLogger(LogStatsJob.class);

	private static final String localTopologyName = "logstats-local";
	
	private static final String clusterTopologyName = "logstats-cluster";
	
	private boolean isLocal = true;
	
	private TopologyBuilder builder;
	
	private StormTopology topology;
	
	private TridentTopology tridentTopology;
	
	private Config conf = new Config();
	
	private LocalCluster cluster;
	
	private AstyanaxCnxn astyanaxCnxn;
	
	

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

		// -i [LOG_ENTRY: {ApacheLogEntry}] -o [LOG_ENTRY: {ApacheLogEntry}, LOG_INDEX_ID: {String}]
		builder.setBolt("indexerBolt", new IndexerBolt(), 1).shuffleGrouping("logRules");

		builder.setBolt("LogStatsMinuteBolt", new LogStatsMinuteBolt(), 1).shuffleGrouping("logRules");
		
		
		setupCassandra();
		
		
		

		builder.setBolt("cassandraMinuteBolt", buildCassandraMinuteBolt(), 1).shuffleGrouping("LogStatsMinuteBolt");
	}
	
	private void buildTopology() throws ConfigException, ConnectionException {
		createBuilder();
		topology = builder.createTopology();
	}
	
	private void createTridentTopology() throws ConfigException, ConnectionException {
		RedisFixedBatchSpout redisTridentSpout = new RedisLPOPFixedBatchSpout(new Fields(LogConstants.LOG_TEXT), 5);
		
		tridentTopology = new TridentTopology();
		// -o LOG_TEXT: {String}
		Stream logRulesStream = tridentTopology.newStream("redisTridentSpout", redisTridentSpout)
			// -i [LOG_TEXT: {String}] -o [LOG_ENTRY: {ApacheLogEntry}]
			.each(new Fields(LogConstants.LOG_TEXT), new LogRulesFunction(), new Fields(LogConstants.LOG_ENTRY));
		
		// -i [LOG_ENTRY: {ApacheLogEntry}] -o [LOG_ENTRY_INDEXER: {ApacheLogEntry}, LOG_INDEX_ID: {String}]
		logRulesStream.each(new Fields(LogConstants.LOG_ENTRY), new IndexerFunction(), new Fields(LogConstants.LOG_ENTRY_INDEXER, LogConstants.LOG_INDEX_ID));
		
		// -i [LOG_ENTRY: {ApacheLogEntry}] -o [FIELD_ROW_KEY: {long}, FIELD_INCREMENT: {1L}, FIELD_COLUMN: {ApacheLogEntry}]
		Stream volumnCountingStream = logRulesStream.each(new Fields(LogConstants.LOG_ENTRY), 
				new VolumeCountingFunction(), 
				new Fields(VolumeCountingFunction.FIELD_ROW_KEY, VolumeCountingFunction.FIELD_INCREMENT, VolumeCountingFunction.FIELD_COLUMN))
			//.groupBy(new Fields(VolumeCountingFunction.FIELD_ROW_KEY)).persistentAggregate(new CassandraStateFactory(inputPath), new Count(), new Fields("count"))
			;
		//for test
		volumnCountingStream.each(new Fields(VolumeCountingFunction.FIELD_ROW_KEY, VolumeCountingFunction.FIELD_INCREMENT, VolumeCountingFunction.FIELD_COLUMN), 
				new WriteToFileFunction("volumnCounting.out").setInputFields(VolumeCountingFunction.FIELD_ROW_KEY, VolumeCountingFunction.FIELD_INCREMENT, VolumeCountingFunction.FIELD_COLUMN),
				new Fields("volumnCountingText"));
		
		//group FIELD_COLUMN
		//setupCassandra();
		
		
		
		//volumnCountingStream.groupBy(new Fields(VolumeCountingFunction.FIELD_ROW_KEY))
		//	.persistentAggregate(new CassandraStateFactory(AstyanaxCnxn.CASSANDRA_CONFIG_KEY), new Count(), new Fields("count"));
		
		
	}
	

	private void buildTridentTopology() throws ConfigException, ConnectionException {
		createTridentTopology();
		topology = tridentTopology.build();
	}
	
	private void setupCassandra() throws ConfigException, ConnectionException {
		astyanaxCnxn = new AstyanaxCnxn();
		astyanaxCnxn.connect();
		astyanaxCnxn.createKeyspaceIfNotExists();
		
		LogStatsColumnFamily logStatsCF = new LogStatsColumnFamily(astyanaxCnxn);
		logStatsCF.createAllCounterCFIfNotExist();
		//LogStatsOptions logStatsOptions = new LogStatsOptions(duration);
		//Map<String, Object> options = logStatsOptions.getCFOptions();
		
		ColumnFamily<String, String> CF_MINUTE_COUNTER_SUPER = new ColumnFamily<String, String>(
	            "LoggingMinuteCounterSuper", StringSerializer.get(), StringSerializer.get());
		
		
		astyanaxCnxn.createCounterCFIfNotExists(CF_MINUTE_COUNTER_SUPER);
		
		setupCassandraConfig();
	}
	
	private void setupCassandraConfig() {
		
		Map<String, Object> cassandraConfig = new HashMap<String, Object>();
		cassandraConfig.put(StormCassandraConstants.CASSANDRA_HOST, "127.0.0.1:9160");
		cassandraConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String[] { astyanaxCnxn.getConfig().getKeySpace() }));
		conf.put(AstyanaxCnxn.CASSANDRA_CONFIG_KEY, cassandraConfig);
		
	}
	
	
	@SuppressWarnings({"rawtypes", "unchecked"})
	private CassandraCounterBatchingBolt buildCassandraMinuteBolt() throws ConnectionException, ConfigException {
		
		setupCassandra();
		
		CassandraCounterBatchingBolt logPersistenceBolt = new CassandraCounterBatchingBolt(
				AstyanaxCnxn.CASSANDRA_CONFIG_KEY,
				new DefaultTupleCounterMapper(
						astyanaxCnxn.getConfig().getKeySpace(), 
						LogConstants.CASSANDRA_MINUTE_COUNT_SUPER_CF_NAME, 
						VolumeCountingBolt.FIELD_ROW_KEY,
						VolumeCountingBolt.FIELD_INCREMENT)
				);
		logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
		
		return logPersistenceBolt;
	}
	
	/*
	@SuppressWarnings({"rawtypes", "unchecked", "unused"})
	private CassandraBatchingBolt buildCassandraBatchingBolt() throws ConnectionException, ConfigException {
		
		setupCassandra();
		
		CassandraBatchingBolt cassandraBatchingBolt = new CassandraBatchingBolt(
				AstyanaxCnxn.CASSANDRA_CONFIG_KEY, 
				new DefaultTupleMapper(astyanaxCnxn.getConfig().getKeySpace(), 
						LogConstants.CASSANDRA_MINUTES_COUNT_CF_NAME, 
						VolumeCountingBolt.FIELD_ROW_KEY));
		cassandraBatchingBolt.setAckStrategy(AckStrategy.ACK_ON_WRITE);
		
		return cassandraBatchingBolt;
	}
	*/
	
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
