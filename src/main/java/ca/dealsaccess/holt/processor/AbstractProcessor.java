package ca.dealsaccess.holt.processor;

import java.util.Properties;

import ca.dealsaccess.holt.processor.DefaultProcessor;

public abstract class AbstractProcessor implements Processor {

	protected Properties parameters;
	
	public AbstractProcessor() {
		
	}
	
	public AbstractProcessor(Properties parameters) {
		this.parameters = parameters;
	}

	public static String getClassName(String customProcessor) {
		if(customProcessor == null) {
			customProcessor = DefaultProcessor.class.getName();
		}
		return customProcessor;
	}
	
	public static AbstractProcessor createFactory(Properties parameters, String processorKey) {
		try {
			String processorClassName = getClassName((String) parameters.get(processorKey));
			return (AbstractProcessor) Class.forName(processorClassName).getConstructor(Properties.class).newInstance(parameters);
		} catch (Exception e) {
			return (AbstractProcessor) new DefaultProcessor(parameters);
		}
	}
}
