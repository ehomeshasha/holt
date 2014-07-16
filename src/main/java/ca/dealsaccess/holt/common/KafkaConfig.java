package ca.dealsaccess.holt.common;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.common.AbstractConfig;

public class KafkaConfig extends AbstractConfig {
	
	private static final Logger LOG = LoggerFactory.getLogger(KafkaConfig.class);

	private static final String DEFAULT_CONFIG_FILENAME = "kafka-conf.properties";
	
	private static final String KAFKA_CONFIG_FILENAME = "ca.dealsaccess.holt.kafka.KafkaConfigFileName";
	
	private static String CONFIG_FILENAME;
	
	static {
		CONFIG_FILENAME = System.getProperty(KAFKA_CONFIG_FILENAME);
		if(CONFIG_FILENAME == null) {
			CONFIG_FILENAME = DEFAULT_CONFIG_FILENAME;
		}
	}
	
	public KafkaConfig() throws ConfigException {
		super(CONFIG_FILENAME);
	}
	
	public KafkaConfig(boolean isResources) throws ConfigException {
		super(CONFIG_FILENAME, isResources);
	}
	
	public KafkaConfig(String configFile) throws ConfigException {
		super(configFile);
	}

	public KafkaConfig(String configFile, boolean isResources) throws ConfigException {
		super(configFile, isResources);
	}
	
	
	
	private String zkHostsString = null;

	private List<String> zkHostsList = null;
	
	private String zkServerString = null;
	
	private List<String> zkServersList = null;
	
	private int zkPort = -1;
	
	private String kafkaTopic = null;
	
	@Override
	public void parseProperties() throws ConfigException {
		
		LOG.info("load kafka config from {}", CONFIG_FILENAME);
		for(Entry<Object, Object> entry : prop.entrySet()) {
			String key = (String) entry.getKey();
			String value = (String) entry.getValue();
			if(key.equals("zk.hosts")) {
				zkHostsString = value.trim();
				zkHostsList = Arrays.asList(zkHostsString.split(","));
			} else if(key.equals("zk.servers")) {
				zkServerString = value.trim();
				zkServersList = Arrays.asList(zkServerString.split(","));
			} else if(key.equals("zk.port")) {
				zkPort = Integer.parseInt(value.trim());
			} else {
				
			}
		}
		
		if(zkHostsString == null) {
			throw new ConfigException("zkHosts not set");
		}
		if(zkServerString == null) {
			throw new ConfigException("zkServers not set");
		}
		if(zkPort == -1) {
			throw new ConfigException("zkPort not set");
		}
	}

	public String getZkHostsString() {
		return zkHostsString;
	}
	
	public List<String> getZkHostsList() {
		return zkHostsList;
	}
	
	public String getZkServersString() {
		return zkServerString;
	}
	
	public List<String> getZkServersList() {
		return zkServersList;
	}
	
	public int getZkPort() {
		return zkPort;
	}

	public String getKafkaTopic() {
		return kafkaTopic;
	}
	
	public String getConfigFilePath() {
		return CONFIG_FILENAME;
	}

	public void setKafkaTopic(String topic) {
		kafkaTopic = topic;
	}
	
}
