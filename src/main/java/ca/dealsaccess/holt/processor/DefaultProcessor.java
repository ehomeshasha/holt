package ca.dealsaccess.holt.processor;

import java.util.Properties;

public class DefaultProcessor extends AbstractProcessor implements Processor {

	
	public DefaultProcessor(Properties parameters) {
		super(parameters);
	}
	
	@Override
	public Object process(Object data) {
		return data;
	}
}
