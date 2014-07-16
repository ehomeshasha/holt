package ca.dealsaccess.holt.filter;

public interface Filter {
	
	/**
	 * 
	 * @param data
	 * @return if data is valid, return true, otherwise return false.
	 */
	public boolean filter(Object data);
}
