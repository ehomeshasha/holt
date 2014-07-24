package ca.dealsaccess.holt.astyanax;

import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.CassandraConfig;

import com.google.common.collect.ImmutableMap;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

public class AstyanaxCnxn {
	
	private CassandraConfig config;
	
	private AstyanaxContext<Keyspace> context;
	
	private Keyspace keyspace;
	
	public static final String CASSANDRA_CONFIG_KEY = "cassandra.logging.config";
	
	public static final String CASSANDRA_MINUTES_COUNT_CF_NAME = "loggingCF";
	
	public AstyanaxCnxn() throws ConfigException {
		config = new CassandraConfig(true);
		config.parseProperties();
	}
	
	public void connect() {
		
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
	
	public void createKeyspaceIfNotExists() throws ConnectionException {
		// Using simple strategy
		
		keyspace.createKeyspaceIfNotExists(ImmutableMap.<String, Object>builder()
		    .put("strategy_options", ImmutableMap.<String, Object>builder()
		        .put("replication_factor", "1")
		        .build())
		    .put("strategy_class",     "SimpleStrategy")
		        .build()
		     );

		
	}

	public CassandraConfig getConfig() {
		return config;
	}

	public AstyanaxContext<Keyspace> getContext() {
		return context;
	}

	public Keyspace getKeyspace() {
		return keyspace;
	}

	public void createCFIfNotExists(String cassandraMinutesCountCfName) throws ConnectionException {
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(cassandraMinutesCountCfName);
        if (cfDef == null) {
        	ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
    				.newColumnFamily(cassandraMinutesCountCfName, StringSerializer.get(), StringSerializer.get());
        	keyspace.createColumnFamily(CF_STANDARD1, null);
        }
	}
}
