package ca.dealsaccess.holt.redis;

import backtype.storm.Config;
import ca.dealsaccess.holt.common.RedisConfig;
import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;

public class RedisUtils {
	
	public static void configure(Config conf) throws ConfigException {
		RedisConfig redisConfig = new RedisConfig(true);
		redisConfig.parseProperties();
		String host = redisConfig.getRedisHost() == null ? RedisConstants.REDIS_HOST_DEFAULT_VALUE : redisConfig.getRedisHost();
		String port = redisConfig.getRedisPort() == -1 ? RedisConstants.REDIS_PORT_DEFAULT_VALUE : String.valueOf(redisConfig.getRedisPort());
		conf.put(RedisConstants.REDIS_HOST, host);
		conf.put(RedisConstants.REDIS_PORT, port);
	}
	
	public static RedisConfig getRedisConfig() throws ConfigException {
		RedisConfig redisConfig = new RedisConfig(true);
		redisConfig.parseProperties();
		return redisConfig;
	}
	
	
	
	
}
