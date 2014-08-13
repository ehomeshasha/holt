package ca.dealsaccess.holt.redis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import backtype.storm.tuple.Values;

import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.util.GsonUtils;
import redis.clients.jedis.Jedis;


public class RedisDataTest {
	
	private Jedis jedis;
	
	private static final String redisHost = "127.0.0.1";
	
	private static final int redisPort = 6379;
	
	private static final String key = "access-log";
	
	@Test
	public void fetchListbyKey() {
		
		jedis = new Jedis(redisHost, redisPort);
		jedis.connect();
		//jedis.flushAll();
		long len = jedis.llen(key);
		List<String> list = jedis.lrange(key, 0, len);
		for(String log : list) {
			System.out.println(log);
		}
		jedis.disconnect();
		//GsonUtils.print(list);
		
	}
	
	
	
	
	
	@Test
	public void writeTestLogToRedis() throws IOException {
		jedis = new Jedis(redisHost, redisPort);
		jedis.connect();
		
		BufferedReader br = new BufferedReader(new FileReader("test.log"));
		String line = null;
		int i=0;
		System.out.println("Write data below to redis:");
		while((line = br.readLine()) != null) {
			i++;
			System.out.println(i+": "+line);
			jedis.rpush(key, line);
		}
		br.close();
		jedis.disconnect();
	}
	@Test
	public void printResults() {
		for(String duration : LogConstants.LOG_DURATION) {
			printDataMap(duration);
		}
	}
	
	@Test
	public void persistenceMinuteALL() throws IOException {
		jedis = new Jedis(redisHost, redisPort);
		jedis.connect();
		
		
		String duration = "Minute";
		
		BufferedReader br = new BufferedReader(new FileReader("test.log"));
		String line = null;
		int i=0;
		while((line = br.readLine()) != null) {
			ApacheLogEntry entry = new ApacheLogEntry(line);
			entry.parseLogText();
			RedisCnxn redisCnxn = new RedisCnxn(jedis, entry, duration, null, null);
			redisCnxn.persistence();
			
			
			
		}
		
		printDataMap(duration);
		
		
		br.close();
		jedis.disconnect();
		
		
		
		
		
		
		
		
		
		
		
		
		
	}




	private StringBuilder sb = new StringBuilder();
	
	private static final String delimiter = "$#$";
	
	private static final String ONE = "1";
	
	private void printDataMap(String duration) {
		Map<String, Map<String, String>> map = new HashMap<String, Map<String, String>>();
		for(int i=0;i<LogConstants.LOG_COLUMNS.length;i++) {
			if(!LogConstants.LOG_COLUMNS[i].equals("Method")) {
				continue;
			}
			String key = sb.append(duration).append(delimiter).append(LogConstants.LOG_COLUMNS[i]).toString();
			sb.setLength(0);
			System.out.println(key);
			Map<String, String> dataMap = jedis.hgetAll(key);
			map.put(key, dataMap);
			
			
		}
		GsonUtils.print(map);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
}