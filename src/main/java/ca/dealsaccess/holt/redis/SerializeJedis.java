package ca.dealsaccess.holt.redis;

import java.io.Serializable;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

public class SerializeJedis extends Jedis implements Serializable {

	private static final long serialVersionUID = 1016231091587137834L;

	public SerializeJedis(String host, int port) {
		super(host, port);
	}

	public SerializeJedis(JedisShardInfo shardInfo) {
		super(shardInfo);
	}

	public SerializeJedis(String host, int port, int timeout) {
		super(host, port, timeout);
	}

	public SerializeJedis(String host) {
		super(host);
	}
	
}
