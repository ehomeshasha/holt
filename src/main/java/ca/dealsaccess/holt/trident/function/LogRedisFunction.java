package ca.dealsaccess.holt.trident.function;

import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class LogRedisFunction extends BaseFunction {
    private static final long serialVersionUID = -7887374473326688398L;

    private Jedis jedis;
    
    private String host;
    
    private int port;
    
    private String key;
    
    public LogRedisFunction(String host, int port, String key) {
    	this.host = host;
        this.port = port;
        this.key = key;
    }
    
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		String logText = (String) tuple.getValueByField(LogConstants.LOG_TEXT);
		jedis.rpush(key, logText);
    }
}
