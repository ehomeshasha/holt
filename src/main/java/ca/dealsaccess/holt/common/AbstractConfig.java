package ca.dealsaccess.holt.common;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;






public abstract class AbstractConfig {
	
	protected final File configFile;
	
	protected Properties prop;
	
	private static final String KAFKA_STORM_CONFIG_CLASS = "ca.dealsaccess.vchannel.kafka.KafkaStormConfigClassName";
	
	@SuppressWarnings("serial")
	public static class ConfigException extends Exception {
		public ConfigException(String msg) {
			super(msg);
		}

		public ConfigException(String msg, Exception e) {
			super(msg, e);
		}
	}
	
	private void loadProperties(InputStream in) throws ConfigException {
		prop = new Properties();
		try {
			prop.load(in);
		} catch (IOException e) {
			throw new ConfigException("Unable to load properties from config file, exiting abnormally");
		}
	}
	
	public AbstractConfig(String configFile) throws ConfigException {
		this(configFile, false);
	}
	
	public AbstractConfig(String configFile, boolean isResources) throws ConfigException {
		this.configFile = new File(configFile);
		InputStream propsStream;
		
		if(!isResources) {
			try {
				propsStream = new FileInputStream(this.configFile);
			} catch (FileNotFoundException e) {
				throw new ConfigException("config file not found, exiting abnormally");
			}
		} else {
			propsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile);
			if(propsStream == null) {
				throw new ConfigException("config file not found, exiting abnormally");
			}
		}
		loadProperties(propsStream);
	}

	public abstract void parseProperties() throws ConfigException;

	public static String getConfigClassName() {
		String configClassName = System.getProperty(KAFKA_STORM_CONFIG_CLASS);
		if(configClassName == null) {
			configClassName = KafkaConfig.class.getName();
		}
		return configClassName;
	}
	
	public static AbstractConfig createFactory() throws IOException {
		String configClassName = getConfigClassName();
		try {
			return (AbstractConfig) Class.forName(configClassName).newInstance();
		} catch (Exception e) {
			IOException ioe = new IOException();
			ioe.initCause(e);
			throw ioe;
		}
	}

	public static AbstractConfig createFactory(File configFile) throws IOException {
		String configClassName = getConfigClassName();
		try {
			return (AbstractConfig) Class.forName(configClassName).getConstructor(File.class).newInstance(configFile);
		} catch (Exception e) {
			IOException ioe = new IOException("Couldn't instantiate "+configClassName);
			ioe.initCause(e);
			throw ioe;
		}
	}
}
