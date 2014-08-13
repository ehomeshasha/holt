package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.astyanax.LogStatsColumnFamily;
import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.log.AbstractLogStats;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.ApacheLogStats;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.redis.RedisCnxn;

import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnFamily;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class LogStatsRedisBolt extends BaseRichBolt {

	public static Logger LOG = LoggerFactory.getLogger(LogStatsRedisBolt.class);
	
	private OutputCollector collector;
	
	private AstyanaxCnxn astyanaxCnxn = null;
	
	private Keyspace keyspace = null;
	
	private ColumnFamily<String, String> IPCF = null;
	
	private final String duration;
	
	public LogStatsRedisBolt(String duration) {
		this.duration = duration;
	}
	
	
	
	
	private Jedis jedis;
	
    private String host;
    
    private int port;
    
    private String key;

    
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
		host = conf.get(RedisConstants.REDIS_HOST).toString();
        port = Integer.valueOf(conf.get(RedisConstants.REDIS_PORT).toString());
        key = conf.get(RedisConstants.REDIS_KEY).toString();
		jedis = new Jedis(host, port);
		jedis.connect();
	}

	@Override
	public void execute(Tuple input) {
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		RedisCnxn redisCnxn = new RedisCnxn(jedis, entry, duration, collector, input);
		
		redisCnxn.persistence();
		collector.emit(new Values(entry));
		collector.ack(input);
		
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(duration+LogConstants.LOG_ENTRY));
	}

}
