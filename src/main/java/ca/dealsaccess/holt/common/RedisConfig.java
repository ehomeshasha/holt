package ca.dealsaccess.holt.common;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.common.AbstractConfig;

public class RedisConfig extends AbstractConfig {
	
	private static final Logger LOG = LoggerFactory.getLogger(RedisConfig.class);

	private static final String DEFAULT_CONFIG_FILENAME = "redis-conf.properties";
	
	private static final String REDIS_CONFIG_FILENAME = "ca.dealsaccess.holt.redis.RedisConfigFileName";
	
	private static String CONFIG_FILENAME;
	
	static {
		CONFIG_FILENAME = System.getProperty(REDIS_CONFIG_FILENAME);
		if(CONFIG_FILENAME == null) {
			CONFIG_FILENAME = DEFAULT_CONFIG_FILENAME;
		}
	}
	
	public RedisConfig() throws ConfigException {
		super(CONFIG_FILENAME);
	}
	
	public RedisConfig(boolean isResources) throws ConfigException {
		super(CONFIG_FILENAME, isResources);
	}
	
	public RedisConfig(String configFile) throws ConfigException {
		super(configFile);
	}

	public RedisConfig(String configFile, boolean isResources) throws ConfigException {
		super(configFile, isResources);
	}
	
	
	
	private String redisHost = null;

	private int redisPort = -1;
	
	@Override
	public void parseProperties() throws ConfigException {
		
		LOG.info("load redis config from {}", CONFIG_FILENAME);
		for(Entry<Object, Object> entry : prop.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			if(key.equals("redis.host")) {
				redisHost = value.trim();
			} else if(key.equals("redis.port")) {
				redisPort = Integer.parseInt(value.trim());
			} else {
				
			}
		}
	}

	public String getRedisHost() {
		return redisHost;
	}

	public int getRedisPort() {
		return redisPort;
	}

	
	
}
