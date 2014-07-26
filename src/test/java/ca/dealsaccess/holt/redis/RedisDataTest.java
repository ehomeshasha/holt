package ca.dealsaccess.holt.redis;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.junit.Test;

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
	
}