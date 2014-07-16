package ca.dealsaccess.holt.kafka;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import backtype.storm.spout.SchemeAsMultiScheme;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.KafkaConfig;
import ca.dealsaccess.holt.zookeeper.ZKClient;


public class KafkaSpoutFactory {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaSpoutFactory.class);
	
	
	public static OpaqueTridentKafkaSpout createOpaqueTridentKafkaSpout(KafkaConfig config) throws ConfigException {
		BrokerHosts brokerHosts = new ZkHosts(config.getZkHostsString());
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(brokerHosts, config.getKafkaTopic());
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
		return kafkaSpout;
	}
	
	public static KafkaSpout createKafkaSpout(KafkaConfig config) throws ConfigException, IOException {
		
		String hosts = config.getZkHostsString();
		String topic = config.getKafkaTopic();
		String zkRoot = "/"+config.getKafkaTopic();
		String id = "storm";
		String[] spoutConifgParameters = { hosts, topic, zkRoot, id};
		LOG.info("SpoutConfig: {zkHosts: {}, topic: {}, zkRoot: {}, id: {}}", spoutConifgParameters);
		
		ZKClient zkClient = new ZKClient(config.getZkHostsList());
		zkClient.createZnode(zkRoot+"/"+id);
		
		BrokerHosts brokerHosts = new ZkHosts(config.getZkHostsString());
		SpoutConfig kafkaConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		kafkaConfig.zkServers = config.getZkServersList();
		kafkaConfig.zkPort = config.getZkPort();
		
		return new KafkaSpout(kafkaConfig); 
	}
	
	
}
