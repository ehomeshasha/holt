package ca.dealsaccess.holt.cassandra;


import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ca.dealsaccess.holt.astyanax.AstyanaxCnxn;
import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.CassandraConfig;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.RangeBuilder;

public class CassandraDataTest {

	private CassandraConfig config;
	
	private AstyanaxContext<Keyspace> context;
	
	private Keyspace keyspace;
	
	private ColumnFamily<String, String> CF_STANDARD1;
	
	@Before
	public void setup() throws ConfigException {
		config = new CassandraConfig(true);
		config.parseProperties();
		connect();
	}
	
	public void connect() {
		
		context = new AstyanaxContext.Builder()
	    	.forCluster(config.getClusterName())
	    	.forKeyspace(config.getKeySpace())
	    	.withAstyanaxConfiguration(
	    			new AstyanaxConfigurationImpl()      
	    			.setDiscoveryType(NodeDiscoveryType.RING_DESCRIBE)
	    			.setCqlVersion(config.getCqlVersion())
	    			.setTargetCassandraVersion(config.getVersion())
	    	)
	    	.withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl(config.getConnectionPool())
	    		.setPort(config.getPort())
	    		.setMaxConnsPerHost(1)
	    		.setSeeds(config.getHost()+":"+config.getPort())
	    	)
	    	.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
	    	.buildKeyspace(ThriftFamilyFactory.getInstance());

		context.start();
		keyspace = context.getClient();
		
		
		CF_STANDARD1 = ColumnFamily
				.newColumnFamily(AstyanaxCnxn.CASSANDRA_MINUTES_COUNT_CF_NAME, StringSerializer.get(), StringSerializer.get());
		
	}
	
	
	@After
	public void shutdown() {
		context.shutdown();
	}
	
	@Test
	public void viewDataTest() throws ConnectionException {
		Rows<String, String> rows = getRows();
		int i=0;
		for (Row<String, String> row : rows) {
		    System.out.println("ROW: " + row.getKey()+ ", columnSize: "+row.getColumns().size());
		    for(Column<String> column: row.getColumns()) {
		    	System.out.println("\t"+column.getName()+": "+column.getStringValue());
		    }
		    i++;
		}
		System.out.println(i+ " rows total.");
	}
	
	@Test
	public void dropCF() throws ConnectionException {
		System.out.println("drop columnFamily "+AstyanaxCnxn.CASSANDRA_MINUTES_COUNT_CF_NAME+".");
		keyspace.dropColumnFamily(CF_STANDARD1);
	}
	
	@Test
	public void createCF() throws ConnectionException {
		System.out.println("create columnFamily "+AstyanaxCnxn.CASSANDRA_MINUTES_COUNT_CF_NAME+".");
		keyspace.createColumnFamily(CF_STANDARD1, null);
	}
	
	@Test
	public void clearDataTest() throws ConnectionException {
		dropCF();
		createCF();
	}
	
	
	@Test
	public void clearDataTestButKeyExists() throws ConnectionException {

		List<String> rowKeysList = getRowKeysList();
		System.out.println("prepare to delete "+rowKeysList.size()+" rows");
        final Iterator<String> rowKeys = rowKeysList.iterator();

        int batchSize = 1000;  
        int nThreads = 10;
        final ExecutorService threadPool = Executors.newFixedThreadPool(nThreads);

        MutationBatch currentBatch = keyspace.prepareMutationBatch();
        int currentBatchSize = 0;

        while (rowKeys.hasNext()) {

            String rowKey = rowKeys.next();
            currentBatch.withRow(CF_STANDARD1, rowKey).delete();
            currentBatchSize++;
            
            if (!rowKeys.hasNext() || (currentBatchSize > batchSize)) {
            	System.out.println("submit batch");
                threadPool.submit(new MutationBatchExec(currentBatch));
                currentBatch = keyspace.prepareMutationBatch();
                currentBatchSize = 0;
            }
        }
    }

    private class MutationBatchExec implements Callable<Void> {
        private final MutationBatch myBatch;
        private MutationBatchExec(MutationBatch batch) {
            myBatch = batch;
        }
        @Override
        public Void call() throws Exception {
        	System.out.println("execute batch");
            myBatch.execute();
            return null;
        }
    }
    
	
	
    private Rows<String, String> getRows() {
		Rows<String, String> rows = null;
		try {
		    rows = keyspace.prepareQuery(CF_STANDARD1)
		        .getAllRows()
		        .setRowLimit(10)
		        .withColumnRange(new RangeBuilder().setLimit(10).build())
		        .setExceptionCallback(new ExceptionCallback() {
		             @Override
		             public boolean onException(ConnectionException e) {
		                 try {
		                     Thread.sleep(1000);
		                 } catch (InterruptedException e1) {
		                 }
		                 return true;
		             }})
		        .execute().getResult();
		} catch (ConnectionException e) {
		}

		
		return rows;
	}
	
	private List<String> getRowKeysList() throws ConnectionException {
	
		OperationResult<Rows<String, String>> result =
			keyspace.prepareQuery(CF_STANDARD1)
			  .getAllRows()
			  .withColumnRange(new RangeBuilder().setLimit(0).build())  // RangeBuilder will be available in version 1.13
			  .execute();
		List<String> rowKeysList = new ArrayList<String>();
		for (Row<String, String> row : result.getResult()) {
			rowKeysList.add(row.getKey());
		}
		
		return rowKeysList;
	
	
	}
	
	
	
}
