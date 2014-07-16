package ca.dealsaccess.holt.filter;

import java.util.Properties;

import ca.dealsaccess.holt.filter.DefaultFilter;
import ca.dealsaccess.holt.processor.DefaultProcessor;

public abstract class AbstractFilter implements Filter {

	protected Properties parameters;
	
	
	public AbstractFilter() {
		
	}
	
	public AbstractFilter(Properties parameters) {
		this.parameters = parameters;
	}

	public static String getClassName(String customProcessor) {
		if(customProcessor == null) {
			customProcessor = DefaultProcessor.class.getName();
		}
		return customProcessor;
	}
	
	public static AbstractFilter createFactory(Properties parameters, String processorKey) {
		String customProcessor = (String) parameters.get(processorKey);
		String processorClassName = getClassName(customProcessor);
		try {
			return (AbstractFilter) Class.forName(processorClassName).getConstructor(Properties.class).newInstance(parameters);
		} catch (Exception e) {
			return (AbstractFilter) new DefaultFilter(parameters);
		}
	}

}
