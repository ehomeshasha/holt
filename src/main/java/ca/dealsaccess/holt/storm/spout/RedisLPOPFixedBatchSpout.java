package ca.dealsaccess.holt.storm.spout;

import java.util.List;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

@SuppressWarnings("serial")
public class RedisLPOPFixedBatchSpout extends RedisFixedBatchSpout {

	
	public RedisLPOPFixedBatchSpout(Fields fields, int maxBatchSize) {
		super(fields, maxBatchSize);
	}

	protected String getData() {
		
		String data = jedis.lpop(key);
		if(data == null || "nil".equals(data)) {
			try {
				Thread.sleep(300);
			} catch (InterruptedException e) {}
			return null;
		}
		return data;
		
	}

	@Override
	protected void addToBatch(List<List<Object>> batch) {
		String data = null;
    	while(batch.size() <= maxBatchSize && (data = getData()) != null ) {
    		batch.add(new Values(data));
    	}
	}

}
