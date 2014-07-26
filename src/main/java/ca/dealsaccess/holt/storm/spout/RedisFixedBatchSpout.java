package ca.dealsaccess.holt.storm.spout;



import backtype.storm.Config;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import ca.dealsaccess.holt.common.RedisConstants;
import redis.clients.jedis.Jedis;
import storm.trident.operation.TridentCollector;
import storm.trident.spout.IBatchSpout;


@SuppressWarnings({"serial", "rawtypes"})
public abstract class RedisFixedBatchSpout implements IBatchSpout {

    protected Fields fields;
    
    protected int maxBatchSize;
    
    protected HashMap<Long, List<List<Object>>> batches = new HashMap<Long, List<List<Object>>>();
    
    public RedisFixedBatchSpout(Fields fields, int maxBatchSize) {
    	this(fields, maxBatchSize, false);
    }
    
    public RedisFixedBatchSpout(Fields fields, int maxBatchSize, boolean cycle) {
    	this.fields = fields;
        this.maxBatchSize = maxBatchSize;
        this.cycle = cycle;
	}

	protected Jedis jedis;
    
    protected String key;
    
    protected boolean cycle = false;
    //protected abstract String getData();
    
    protected abstract void addToBatch(List<List<Object>> batch);
    
    @Override
    public void open(Map conf, TopologyContext context) {
        key = (String) conf.get(RedisConstants.REDIS_KEY);
    	String host = (String) conf.get(RedisConstants.REDIS_HOST);
    	int port = Integer.parseInt((String) conf.get(RedisConstants.REDIS_PORT));
    	jedis = new Jedis(host, port);
    	jedis.connect();
    }

    
    
    @Override
    public void emitBatch(long batchId, TridentCollector collector) {
        List<List<Object>> batch = this.batches.get(batchId);
        
        if(batch == null) {
        	batch = new ArrayList<List<Object>>();
        	
        	addToBatch(batch);
        	
        	this.batches.put(batchId, batch);
        }
        for(List<Object> list : batch){
            collector.emit(list);
        }
    }

    @Override
    public void ack(long batchId) {
        this.batches.remove(batchId);
    }

    @Override
    public void close() {
    	jedis.disconnect();
    }

    @Override
    public Map getComponentConfiguration() {
        Config conf = new Config();
        conf.setMaxTaskParallelism(1);
        return conf;
    }

    @Override
    public Fields getOutputFields() {
        return fields;
    }
    
}