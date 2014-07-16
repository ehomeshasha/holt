package ca.dealsaccess.holt.flumekafka;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;


/**
 * The type Single partition.
 */
public class SinglePartition implements Partitioner {

	//private static final Logger LOGGER = LoggerFactory.getLogger(SinglePartition.class);

    public SinglePartition(VerifiableProperties props) {
    	
    }

    @Override
	public int partition(Object obj, int numberOfPartions) {
		return 0;
	}

}
