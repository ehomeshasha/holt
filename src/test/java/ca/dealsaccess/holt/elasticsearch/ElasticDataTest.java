package ca.dealsaccess.holt.elasticsearch;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ca.dealsaccess.holt.trident.function.IndexerFunction;
import ca.dealsaccess.holt.util.GsonUtils;
import static org.elasticsearch.index.query.QueryBuilders.*;
public class ElasticDataTest {

	private static Node node;
	
	private static Client client;
	
	@BeforeClass
	public static void setup() {
		node = NodeBuilder.nodeBuilder().node();
		client = node.client();
	}
	
	
	@AfterClass
	public static void shutdown() {
		node.close();
	}
	
	@Test
	public void clearDataTest() {
		
		@SuppressWarnings("unused")
		DeleteByQueryResponse response = client.prepareDeleteByQuery(IndexerFunction.INDEX_NAME)
		        .setQuery(matchAllQuery())
		        .execute()
		        .actionGet();
	}
	
	
	
	@SuppressWarnings("rawtypes")
	@Test
	public void viewDataTest() {
		SearchResponse response = client.prepareSearch(IndexerFunction.INDEX_NAME)
		        .setTypes(IndexerFunction.INDEX_TYPE).setSize(1000)
		        .execute()
		        .actionGet();
		
		Map<String, Map> map = new HashMap<String, Map>();
		
		for(SearchHit hit :response.getHits()) {
			map.put(hit.getId(), hit.getSource());
		}
		System.out.println("Total: "+response.getHits().getTotalHits());
		GsonUtils.print(map);
	}
	
}
