package ca.dealsaccess.holt.common;

import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLConfig extends AbstractConfig {

	private static final Logger LOG = LoggerFactory.getLogger(MySQLConfig.class);

	private static final String DEFAULT_CONFIG_FILENAME = "mysql-conf.properties";
	
	private static final String MYSQL_CONFIG_FILENAME = "ca.dealsaccess.holt.mysql.MySQLConfigFileName";
	
	private static String CONFIG_FILENAME;
	
	static {
		CONFIG_FILENAME = System.getProperty(MYSQL_CONFIG_FILENAME);
		if(CONFIG_FILENAME == null) {
			CONFIG_FILENAME = DEFAULT_CONFIG_FILENAME;
		}
	}
	
	public MySQLConfig() throws ConfigException {
		super(CONFIG_FILENAME);
	}
	
	public MySQLConfig(boolean isResources) throws ConfigException {
		super(CONFIG_FILENAME, isResources);
	}
	
	public MySQLConfig(String configFile) throws ConfigException {
		super(configFile);
	}

	public MySQLConfig(String configFile, boolean isResources) throws ConfigException {
		super(configFile, isResources);
	}
	
	private static final String JDBC = "jdbc:mysql://";
	
	private String host = null;
	
	private String port = "3306";
	
	private String database = null;
	
	private String username = null;
	
	private String password = null;

	@Override
	public void parseProperties() throws ConfigException {
		
		LOG.info("load mysql config from {}", CONFIG_FILENAME);
		for(Entry<Object, Object> entry : prop.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			if(key.equals("mysql.host")) {
				host = value.trim();
			} else if(key.equals("mysql.port")) {
					port = value.trim();
			} else if(key.equals("mysql.database")) {
				database = value.trim();
			} else if(key.equals("mysql.username")) {
				username = value.trim();
			} else if(key.equals("mysql.password")) {
				password = value.trim();
			} else {
				
			}
		}
	}

	public String getHost() {
		return host;
	}

	public String getDatabase() {
		return database;
	}

	public String getUsername() {
		return username;
	}

	public String getPassword() {
		return password;
	}

	public String getPort() {
		return port;
	}

	public String getJDBC() {
		return JDBC;
	}

}
