package ca.dealsaccess.holt.redis;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import redis.clients.jedis.Jedis;
import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import ca.dealsaccess.holt.astyanax.LogStatsColumnFamily;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.CassandraConfig;
import ca.dealsaccess.holt.common.MySQLConfig;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.util.GsonUtils;

import com.google.common.collect.ImmutableMap;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class RedisCnxn {

	public static final String LOGGING = "Logging";

	public static final String COUNTER = "Counter";
	
	public static final String IP_TABLE = "IP_TABLE";
	
	private Jedis jedis;
	
	private ApacheLogEntry entry;
	
	private String duration;
	
	private OutputCollector collector;
	
	private Tuple tuple;
	
	private static final String delimiter = "$#$";
	
	private static final String ONE = "1";
	
	public RedisCnxn(Jedis jedis) {
		this.jedis = jedis;
	}

	public RedisCnxn(Jedis jedis, ApacheLogEntry entry, String duration, OutputCollector collector, Tuple tuple) {
		this.jedis = jedis;
		this.entry = entry;
		this.duration = duration;
		this.collector = collector;
		this.tuple = tuple;
	}

	private StringBuilder sb = new StringBuilder();
	
	public void persistence() {
		
		String[] columnArray = ((ApacheLogEntry) entry).getIPNoneExistsArray();
		
		for(int i=0;i<LogConstants.LOG_COLUMNS.length;i++) {
			String innerKey = columnArray[i];
			String key = sb.append(duration).append(delimiter).append(LogConstants.LOG_COLUMNS[i]).toString();
			sb.setLength(0);
			Map<String, String> dataMap = jedis.hgetAll(key);
			if(!dataMap.containsKey(innerKey)) {
				dataMap.put(innerKey, ONE);
			} else {
				dataMap.put(innerKey, Integer.valueOf((Integer.parseInt(dataMap.get(innerKey))+1)).toString());
			}
			jedis.hmset(key, dataMap);
			GsonUtils.print(key, dataMap);
		}
		
		
		
		
		
		
		
		
		
		
		
		
		
	}
	
	
	
}
