package ca.dealsaccess.holt.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegExFilter extends AbstractFilter implements Filter {

	private Pattern pattern;
	
	@Override
	public boolean filter(Object data) {
		if(pattern == null || pattern.toString().isEmpty()) {
			return true;
		}
		Matcher matcher = pattern.matcher((String) data);
        return matcher.find();
	}

	public Pattern getPattern() {
		return pattern;
	}

	public void setPattern(Pattern pattern) {
		this.pattern = pattern;
	}
	
	public void setPattern(String patternStr) {
		this.pattern = Pattern.compile(patternStr);
	}
}
