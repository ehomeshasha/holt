package ca.dealsaccess.holt.integration;

import static org.junit.Assert.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import backtype.storm.utils.Utils;
import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.RedisConfig;
import ca.dealsaccess.holt.jobs.LogStatsJob;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.redis.RedisUtils;
import ca.dealsaccess.holt.storm.bolt.IndexerBolt;
import ca.dealsaccess.holt.storm.bolt.VolumeCountingBolt;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;

/**
 * The integration test basically injects on the input queue, and then
 * introduces a test bolt which simply persists the tuple into a JSON object
 * onto an output queue. Note that test is parameter driven, but the cluster is
 * only shutdown once all tests have run
 * */
public class IntegrationTopologyTest {

	public static final String REDIS_CHANNEL = "TestLogBolt";
	private static final String testRedisKey = "access-log";
	private static Jedis jedis;
	private static LogStatsJob logStatsJob = new LogStatsJob();
	
	private static Client client;
	
	private static RedisConfig redisConfig;
	
	private static Keyspace keyspace;

	private static ColumnFamily<String, String> CF_USER_INFO;
	
	private static AstyanaxContext<Keyspace> context;
	
	@BeforeClass
	public static void setup() throws Exception {
		setupCassandra();
		setupElasticSearch();
		setupRedis();
		setupTopology();
	}

	private static void setupCassandra() throws Exception {
		
		AstyanaxCnxn astyanaxCnxn = new AstyanaxCnxn();
		astyanaxCnxn.connect();
		
		
		context = astyanaxCnxn.getContext();
		keyspace = astyanaxCnxn.getKeyspace();
		
		
		CF_USER_INFO =
				  new ColumnFamily<String, String>(
					AstyanaxCnxn.CASSANDRA_MINUTES_COUNT_CF_NAME,      // Column Family Name
				    StringSerializer.get(),   // Key Serializer
				    StringSerializer.get());
		
	}

	private static void setupElasticSearch() throws Exception {
		Node node = NodeBuilder.nodeBuilder().local(true).node();
		client = node.client();
		Thread.sleep(5000);
	}
	
	private static void setupRedis() throws ConfigException {
		redisConfig = RedisUtils.getRedisConfig();
		// jedis required for input and ouput of the cluster
		jedis = new Jedis(redisConfig.getRedisHost(), redisConfig.getRedisPort());
		jedis.connect();
		jedis.flushDB();
	}

	private static void setupTopology() throws SecurityException, NoSuchMethodException, IllegalArgumentException, IllegalAccessException, InvocationTargetException {
		// We want all output tuples coming to the mock for testing purposes
		String[] args = {"--redisKey", testRedisKey};
		
		//测试带有参数的private方法  
        Method testInitialize = logStatsJob.getClass().getDeclaredMethod("initialize",String[].class);   
        testInitialize.setAccessible(true);   
        //调用  
        testInitialize.invoke(logStatsJob, new Object[] {args});  
        
        
		logStatsJob.runLocal(Integer.MAX_VALUE);
		
		// give it some time to startup before running the tests.
		Utils.sleep(5000);
	}

	@AfterClass
	public static void shutDown() {
		logStatsJob.shutDownLocal();
		jedis.disconnect();
		client.close();
		context.shutdown();
	}

	@Test
	public void inputOutputClusterTest() throws Exception {
		String testData = UnitTestUtils.readFile("test.log");
		jedis.rpush(testRedisKey, testData);
		ApacheLogEntry entry = new ApacheLogEntry(testData);
		entry.parseLogText();
		long minute = VolumeCountingBolt.getMinuteForTime(entry.getTimeStamp());
		Utils.sleep(6000);
		
		// Check that the indexing working
		GetResponse response = client
				.prepareGet(IndexerBolt.INDEX_NAME, IndexerBolt.INDEX_TYPE, testData)
				.execute().actionGet();
		assertTrue(response.exists());
		// now check that count has been updated in cassandra
		Column<String> result = keyspace
				.prepareQuery(CF_USER_INFO)
				.getKey(Long.toString(minute)).getColumn(entry.getLogText())
				.execute().getResult();
		assertEquals(1L, result.getLongValue());

	}

}
