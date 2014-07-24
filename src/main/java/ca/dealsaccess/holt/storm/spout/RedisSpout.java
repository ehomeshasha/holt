package ca.dealsaccess.holt.storm.spout;

import java.util.Map;

import ca.dealsaccess.holt.common.RedisConstants;
import ca.dealsaccess.holt.log.LogConstants;
import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class RedisSpout extends BaseRichSpout {

	private Jedis jedis;

	private String host;

	private int port;

	private String key;

	private SpoutOutputCollector collector;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields(LogConstants.LOG_TEXT));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
		this.collector = spoutOutputCollector;
		host = conf.get(RedisConstants.REDIS_HOST).toString();
		port = Integer.valueOf(conf.get(RedisConstants.REDIS_PORT).toString());
		key = conf.get(RedisConstants.REDIS_KEY).toString();
		jedis = new Jedis(host, port);
	}

	@Override
	public void nextTuple() {
		String logText = jedis.lpop(key);
		if (logText == null || "nil".equals(logText)) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {}
		} else {
			collector.emit(new Values(logText));
		}
	}
}
