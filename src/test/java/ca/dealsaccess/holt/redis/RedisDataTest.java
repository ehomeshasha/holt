package ca.dealsaccess.holt.redis;

import java.util.List;

import org.junit.Test;

import redis.clients.jedis.Jedis;


public class RedisDataTest {
	
	private Jedis jedis;
	
	private static final String redisHost = "127.0.0.1";
	
	private static final int redisPort = 6379;
	
	private static final String key = "access-log";
	
	@Test
	public void fetchRedisListData() {
		
		jedis = new Jedis(redisHost, redisPort);
		jedis.connect();
		//jedis.flushAll();
		long len = jedis.llen(key);
		List<String> list = jedis.lrange(key, 0, len);
		for(String log : list) {
			System.out.println(log);
		}
		//GsonUtils.print(list);
		
	}
	
}