/*package ca.dealsaccess.holt.cassandra;

import java.util.Map;

import org.junit.After;
import org.junit.Before;

import ca.dealsaccess.holt.common.CassandraConfig;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.log.LogConstants;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class CassandraSuperColumnCounter {
private CassandraConfig config;
	
	private AstyanaxContext<Keyspace> context;
	
	private Keyspace keyspace;
	
	
	
	@Before
	public void setup() throws ConfigException, ConnectionException {
		config = new CassandraConfig(true);
		config.parseProperties();
		connect();
	}
	
	public void connect() throws ConnectionException {
		
		context = new AstyanaxContext.Builder()
	    	.forCluster(config.getClusterName())
	    	.forKeyspace(config.getKeySpace())
	    	.withAstyanaxConfiguration(
	    			new AstyanaxConfigurationImpl()      
	    			.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    			.setCqlVersion(config.getCqlVersion())
	    			.setTargetCassandraVersion(config.getVersion())
	    	)
	    	.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(config.getConnectionPool())
	    		.setPort(config.getPort())
	    		.setMaxConnsPerHost(1)
	    		.setSeeds(config.getHost()+":"+config.getPort())
	    	)
	    	.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
	    	.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		keyspace = context.getClient();
		
	}
	
	

	@After
	public void shutdown() {
		context.shutdown();
	}
}
*/