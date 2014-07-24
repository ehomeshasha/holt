package ca.dealsaccess.holt.common;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.common.AbstractConfig;

public class CassandraConfig extends AbstractConfig {
	
	private static final Logger LOG = LoggerFactory.getLogger(CassandraConfig.class);

	private static final String DEFAULT_CONFIG_FILENAME = "cassandra-conf.properties";
	
	private static final String CASSANDRA_CONFIG_FILENAME = "ca.dealsaccess.holt.cassandra.CassandraConfigFileName";
	
	private static String CONFIG_FILENAME;
	
	static {
		CONFIG_FILENAME = System.getProperty(CASSANDRA_CONFIG_FILENAME);
		if(CONFIG_FILENAME == null) {
			CONFIG_FILENAME = DEFAULT_CONFIG_FILENAME;
		}
	}
	
	public CassandraConfig() throws ConfigException {
		super(CONFIG_FILENAME);
	}
	
	public CassandraConfig(boolean isResources) throws ConfigException {
		super(CONFIG_FILENAME, isResources);
	}
	
	public CassandraConfig(String configFile) throws ConfigException {
		super(configFile);
	}

	public CassandraConfig(String configFile, boolean isResources) throws ConfigException {
		super(configFile, isResources);
	}
	private static final String CASSANDRA_HOST_KEY = "cassandra.host";
	
	private static final String CASSANDRA_DEFAULT_HOST = "127.0.0.1";
	
	private static final String CASSANDRA_PORT_KEY = "cassandra.port";
	
	private static final int CASSANDRA_DEFAULT_PORT = 9160;
	
	private static final String CLUSTER_NAME_KEY = "cassandra.cluster.name";
	
	private static final String CLUSTER_DEFAULT_NAME = "ClusterName";
	
	private static final String CONNECTION_POOL_KEY = "cassandra.connectionpool";
	
	private static final String CONNECTION_DEFAULT_POOL = "MyConnectionPool";
	
	private static final String KEYSPACE_KEY = "cassandra.keyspace";
	
	private static final String DEFAULT_KEYSPACE = "logging";
	
	private static final String CQL_VERSION_KEY = "cassandra.cql.version";
	
	private static final String CQL_DEFAULT_VERSION = "3.0.0";
	
	private static final String VERSION_KEY = "cassandra.version";
	
	private static final String DEFAULT_VERSION = "1.2";
	
	
	
	
	private String host = CASSANDRA_DEFAULT_HOST;
	
	private int port = CASSANDRA_DEFAULT_PORT;
	
	private String clusterName = CLUSTER_DEFAULT_NAME;
	
	private String connectionPool = CONNECTION_DEFAULT_POOL;
	
	private String keySpace = DEFAULT_KEYSPACE;
	
	private String cqlVersion = CQL_DEFAULT_VERSION;
	
	private String version = DEFAULT_VERSION;

	
	@Override
	public void parseProperties() throws ConfigException {
		
		LOG.info("load cassandra config from {}", CONFIG_FILENAME);
		for(Entry<Object, Object> entry : prop.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			if(key.equals(CASSANDRA_HOST_KEY)) {
				host = value.trim();
			} else if(key.equals(CASSANDRA_PORT_KEY)) {
				port = Integer.parseInt(value.trim());
			} else if(key.equals(CLUSTER_NAME_KEY)) {
				clusterName = value.trim();
			} else if(key.equals(CONNECTION_POOL_KEY)) {
				connectionPool = value.trim();
			} else if(key.equals(CQL_VERSION_KEY)) {
				cqlVersion = value.trim();
			} else if(key.equals(VERSION_KEY)) {
				version = value.trim();
			} else if(key.equals(KEYSPACE_KEY)) {
				keySpace = value.trim();
			}
		}
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getConnectionPool() {
		return connectionPool;
	}

	public String getCqlVersion() {
		return cqlVersion;
	}

	public String getVersion() {
		return version;
	}

	public String getKeySpace() {
		return keySpace;
	}

	

	
	
}
