package ca.dealsaccess.holt.astyanax;

import java.util.Map;

import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.CassandraConfig;

import com.google.common.collect.ImmutableMap;
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

public class AstyanaxCnxn {
	
	private CassandraConfig config;
	
	private AstyanaxContext<Keyspace> context;
	
	private Keyspace keyspace;
	
	public static final String CASSANDRA_CONFIG_KEY = "cassandra.config";
	
	
	
	private Map<String, Object> CF_COUNTER1_OPTIONS = ImmutableMap.<String, Object>builder()
	        .put("default_validation_class", "CounterColumnType")
	        .put("replicate_on_write", true)
	        .build(); 
	
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
	
	public void close() {
		if(context != null) {
			context.shutdown();
		}
	}
	
	public void createKeyspaceIfNotExists() throws ConnectionException {
		// Using simple strategy
		KeyspaceDefinition ksDef = null;
		try {
			ksDef = keyspace.describeKeyspace();
		} catch (BadRequestException e) {}
		
		if(ksDef == null) {
			keyspace.createKeyspace(ImmutableMap.<String, Object>builder()
			    .put("strategy_options", ImmutableMap.<String, Object>builder()
			        .put("replication_factor", "1")
			        .build())
			    .put("strategy_class",     "SimpleStrategy")
			        .build()
			     );
		}
		
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

	
	
	
	public void createCFIfNotExists(String cfName) throws ConnectionException {
		createCFIfNotExists(cfName, null);
	}
	
	public void createCFIfNotExists(ColumnFamily<?, ?> CF) throws ConnectionException {
		createCFIfNotExists(CF, null);
	}
	
	
	public void createCFIfNotExists(String cfName, Map<String, Object> CF_Options) throws ConnectionException {
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(cfName);
        if (cfDef == null) {
        	ColumnFamily<String, String> CF_STANDARD1 = ColumnFamily
    				.newColumnFamily(cfName, StringSerializer.get(), StringSerializer.get());
        	keyspace.createColumnFamily(CF_STANDARD1, CF_Options);
        }
	}
	
	
	public void createCFIfNotExists(ColumnFamily<?, ?> CF, Map<String, Object> CF_Options) throws ConnectionException {
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(CF.getName());
        if (cfDef == null) {
        	keyspace.createColumnFamily(CF, CF_Options);
        }
        
	}
	
	public void createCounterCFIfNotExists(String cfName) throws ConnectionException {
		createCFIfNotExists(cfName, CF_COUNTER1_OPTIONS);
	}
	
	public void createCounterCFIfNotExists(ColumnFamily<?, ?> CF) throws ConnectionException {
		createCFIfNotExists(CF, CF_COUNTER1_OPTIONS);
        
	}
	
	
	public void createAllCFNeeded() throws ConnectionException {
		LogStatsColumnFamily logCF = new LogStatsColumnFamily(this);
		logCF.createAllCounterCFIfNotExist();
		logCF.createAllIPCFIfNotExist();
	}
	
	public ColumnFamily<String, String> getCFbyName(String cfName) {
		return ColumnFamily.newColumnFamily(cfName, StringSerializer.get(), StringSerializer.get());
	}
	
	
	
		
		
		
	
	
}
