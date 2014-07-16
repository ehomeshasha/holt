package ca.dealsaccess.holt.flumekafka;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ca.dealsaccess.holt.processor.AbstractProcessor;
import ca.dealsaccess.holt.processor.Processor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

/**
 * kafka sink.
 */
public class KafkaSink extends AbstractSink implements Configurable {
    
	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

	private Properties parameters;
    
	private Producer<String, String> producer;
    
	private Context context;
	
	private Processor processor;

    @Override
    public void configure(Context context) {
        this.context = context;
        ImmutableMap<String, String> props = this.context.getParameters();

        parameters = new Properties();
        for (String key : props.keySet()) {
            String value = props.get(key);
            this.parameters.put(key, value);
        }
    }

    @Override
    public synchronized void start() {
        super.start();
        ProducerConfig config = new ProducerConfig(this.parameters);
        this.producer = new Producer<String, String>(config);
        this.processor = AbstractProcessor.createFactory(this.parameters, (String) this.parameters.get(KafkaFlumeConstans.CUSTOME_PROCESSOR));
    }

    
    
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;

        // Start transaction
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {
            // This try clause includes whatever Channel operations you want to do
            Event event = ch.take();

            String partitionKey = (String) parameters.get(KafkaFlumeConstans.PARTITION_KEY_NAME);
            String encoding = StringUtils.defaultIfEmpty(
                    (String) this.parameters.get(KafkaFlumeConstans.ENCODING_KEY_NAME),
                    KafkaFlumeConstans.DEFAULT_ENCODING);
            String topic = Preconditions.checkNotNull(
                    (String) this.parameters.get(KafkaFlumeConstans.CUSTOME_TOPIC_KEY_NAME),
                    "custom.topic.name is required");

            String eventData = new String(event.getBody(), encoding);
            
            eventData = (String) processor.process(eventData);
            
            if(eventData != null) {
            	KeyedMessage<String, String> data;
	
	            if (StringUtils.isEmpty(partitionKey)) {
	                data = new KeyedMessage<String, String>(topic, eventData);
	            } else {
	                data = new KeyedMessage<String, String>(topic, partitionKey, eventData);
	            }
	
	            if (LOGGER.isInfoEnabled()) {
	                LOGGER.info("Send Message to Kafka : [" + eventData + "] -- [" + EventHelper.dumpEvent(event) + "]");
	            }
	
	            producer.send(data);
            }
            
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;

            // re-throw all Errors
            if (t instanceof Error) {
                throw (Error) t;
            }
        } finally {
            txn.close();
        }
        return status;
    }

    @Override
    public void stop() {
        producer.close();
    }
}
