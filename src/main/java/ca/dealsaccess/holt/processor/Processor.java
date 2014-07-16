package ca.dealsaccess.holt.processor;

public interface Processor {
	
	/**
	 * 
	 * @param data
	 * @return processed data from flume
	 */
	
	public Object process(Object data);
	
}
