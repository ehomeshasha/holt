package ca.dealsaccess.holt.trident.function;

import java.util.Map;

import backtype.storm.tuple.Values;
import ca.dealsaccess.holt.common.RedisConstants;
import redis.clients.jedis.Jedis;
import storm.kafka.StringScheme;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

public class LogRedisFunction extends BaseFunction {
    private static final long serialVersionUID = -7887374473326688398L;

    private Jedis jedis;
    
    private String key;
    
    public LogRedisFunction() {
    	
    }
    
    @SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map conf, TridentOperationContext context) {
    	
    	key = (String) conf.get(RedisConstants.REDIS_KEY);
    	String host = (String) conf.get(RedisConstants.REDIS_HOST);
    	int port = Integer.parseInt((String) conf.get(RedisConstants.REDIS_PORT));
    	jedis = new Jedis(host, port);
    	jedis.connect();
    }

    @Override
    public void cleanup() {
    	if(jedis != null) {
    		jedis.disconnect();
    	}
    }
    
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		String logText = (String) tuple.getValueByField(StringScheme.STRING_SCHEME_KEY);
		jedis.rpush(key, logText);
		collector.emit(new Values(logText));
    }
}
