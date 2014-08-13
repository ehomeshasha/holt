package ca.dealsaccess.holt.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisTestUtils {
	
	
	/** 
	 * 获取连接池. 
	 * @return 连接池实例 
	 */  
	public static JedisPool getPool() {  
	    return JedisUtil.getPool("127.0.0.1", 6379);
	}

	public static Jedis getJedis() {
		return JedisUtil.getJedis("127.0.0.1", 6379);
	}

	public static void closeJedis(Jedis jedis) {
		JedisUtil.closeJedis(jedis, "127.0.0.1", 6379);
		
	}  
}
