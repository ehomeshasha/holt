package ca.dealsaccess.holt.trident.function;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.nio.charset.Charset;

import backtype.storm.tuple.Values;

import com.google.common.io.Files;

import ca.dealsaccess.holt.util.GsonUtils;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

@SuppressWarnings("serial")
public class WriteToFileFunction extends BaseFunction {
    
	private String fileName;
	
	private String[] fields = null;
	
	public WriteToFileFunction(String fileName) {
		this.fileName = fileName;
	}
	
	
	@SuppressWarnings("rawtypes")
	@Override
    public void prepare(Map conf, TridentOperationContext context) {
    	
    		
		
		
    }

    @Override
    public void cleanup() {
    	
    }
	
    
    public WriteToFileFunction setInputFields(String... fields) {
    	this.fields = fields;
    	return this;
    }
	
	@Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
		
		//String logText = (String) tuple.getValueByField(LogConstants.LOG_TEXT);
		if(fields == null) {
			fields = new String[tuple.size()];
			for(int j=0;j<tuple.size();j++) {
				fields[j] = String.valueOf(j);
			}
		}
		
		
		Map<String, Object> map = new HashMap<String, Object>();
		
		for(int i=0;i<tuple.size();i++) {
			map.put(fields[i], tuple.getValue(i));
		}
		String text = GsonUtils.toJson(map);
		collector.emit(new Values(text));
		
		try {
			Files.append(text+System.getProperty("line.separator"), new File(fileName), Charset.forName("UTF-8"));
		} catch (IOException e) {}
		
    }
}
