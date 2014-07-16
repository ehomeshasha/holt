package ca.dealsaccess.holt.filter;

import java.util.Properties;

public class DefaultFilter extends AbstractFilter implements Filter {

	public DefaultFilter(Properties parameters) {
		super(parameters);
	}

	@Override
	public boolean filter(Object data) {
		return true;
	}

}
