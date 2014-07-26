package ca.dealsaccess.holt.trident.function;

import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.tuple.Values;
import ca.dealsaccess.holt.log.ApacheLogEntry;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.util.GsonUtils;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("serial")
public class IndexerFunction extends BaseFunction {
    
	
	private static final Logger LOG = LoggerFactory.getLogger(IndexerFunction.class);

	private Node node;
	
	private Client client;

	public static final String INDEX_NAME = "logstorm";
	public static final String INDEX_TYPE = "logentry";
	public static final String ELASTIC_CLUSTER_NAME = "ElasticClusterName";
	public static final String DEFAULT_ELASTIC_CLUSTER = "LogStorm-Cluster";
	
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map conf, TridentOperationContext context) {
		
		if ((Boolean) conf.get(backtype.storm.Config.TOPOLOGY_DEBUG) == true) {
			node = NodeBuilder.nodeBuilder().node();
		} else {
			String clusterName = (String) conf.get(ELASTIC_CLUSTER_NAME);
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
    public void execute(TridentTuple tuple, TridentCollector collector) {
		
		ApacheLogEntry entry = (ApacheLogEntry) tuple.getValueByField(LogConstants.LOG_ENTRY);
		if (entry == null) {
			LOG.error("Received null or incorrect value from tuple");
		} else {
			String toBeIndexed = GsonUtils.toJson(entry);
			IndexResponse response = client.prepareIndex(INDEX_NAME, INDEX_TYPE).setSource(toBeIndexed).execute()
					.actionGet();
			if (response == null) {
				LOG.error("Failed to index Tuple: " + tuple.toString());
			} else {
				if (response.getId() == null)
					LOG.error("Failed to index Tuple: " + tuple.toString());
				else {
					LOG.debug("Indexing success on Tuple: " + tuple.toString());
					collector.emit(new Values(entry, response.getId()));
					
				}
			}
		}
    }
}
