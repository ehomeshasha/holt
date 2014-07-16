package ca.dealsaccess.holt.flumekafka;

import java.util.Properties;

import ca.dealsaccess.holt.filter.RegExFilter;
import ca.dealsaccess.holt.processor.AbstractProcessor;
import ca.dealsaccess.holt.processor.Processor;

public class EventDataProcessor extends AbstractProcessor implements Processor {

	public EventDataProcessor(Properties parameters) {
		super(parameters);
	}

	@Override
	public Object process(Object data) {
		String patternStr = (String) parameters.get("custom.validate.regex.pattern");
		RegExFilter filter = new RegExFilter();
		filter.setPattern(patternStr);
		boolean result = filter.filter(data);
		if(result) {
			return data;
		}
		return null;
	}

}
