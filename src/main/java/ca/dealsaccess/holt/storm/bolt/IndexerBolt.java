package ca.dealsaccess.holt.storm.bolt;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.util.GsonUtils;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class IndexerBolt extends BaseRichBolt {

	private static final Logger LOG = LoggerFactory.getLogger(IndexerBolt.class);

	private Node node;
	
	private Client client;

	private OutputCollector collector;

	public static final String INDEX_NAME = "logstorm";
	public static final String INDEX_TYPE = "logentry";
	public static final String ELASTIC_CLUSTER_NAME = "ElasticClusterName";
	public static final String DEFAULT_ELASTIC_CLUSTER = "LogStorm-Cluster";

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		
		if ((Boolean) stormConf.get(backtype.storm.Config.TOPOLOGY_DEBUG) == true) {
			node = NodeBuilder.nodeBuilder().node();
		} else {
			String clusterName = (String) stormConf.get(ELASTIC_CLUSTER_NAME);
			if (clusterName == null)
				clusterName = DEFAULT_ELASTIC_CLUSTER;
			node = NodeBuilder.nodeBuilder().clusterName(clusterName).node();
		}
		client = node.client();
	}

	@Override
    public void cleanup() {
		node.close();
    } 
	
	@Override
	public void execute(Tuple input) {
		ApacheLogEntry entry = (ApacheLogEntry) input.getValueByField(LogConstants.LOG_ENTRY);
		if (entry == null) {
			LOG.error("Received null or incorrect value from tuple");
		} else {
			String toBeIndexed = GsonUtils.toJson(entry);
			IndexResponse response = client.prepareIndex(INDEX_NAME, INDEX_TYPE).setSource(toBeIndexed).execute()
					.actionGet();
			if (response == null) {
				LOG.error("Failed to index Tuple: " + input.toString());
			} else {
				if (response.getId() == null)
					LOG.error("Failed to index Tuple: " + input.toString());
				else {
					LOG.debug("Indexing success on Tuple: " + input.toString());
					collector.emit(new Values(entry, response.getId()));
					
				}
			}
		}
		collector.ack(input);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(LogConstants.LOG_ENTRY, LogConstants.LOG_INDEX_ID));
	}

}
