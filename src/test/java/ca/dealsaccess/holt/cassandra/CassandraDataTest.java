package ca.dealsaccess.holt.cassandra;


import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.CassandraConfig;
import ca.dealsaccess.holt.log.LogConstants;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ExceptionCallback;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.ColumnFamilyDefinition;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
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
	
	private ColumnFamily<String, String> CF_COUNTER1;
	
	private Map<String, Object> CF_COUNTER1_OPTIONS = ImmutableMap.<String, Object>builder()
	        .put("default_validation_class", "CounterColumnType")
	        .put("replicate_on_write", true)
	        .build(); 
	
	@Before
	public void setup() throws ConfigException, ConnectionException {
		config = new CassandraConfig(true);
		config.parseProperties();
		connect();
	}
	
	public void connect() throws ConnectionException {
		
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
		
		
		CF_COUNTER1 = ColumnFamily
				.newColumnFamily(LogConstants.CASSANDRA_MINUTE_COUNT_CF_NAME, StringSerializer.get(), StringSerializer.get());
		
		
		
		createCFIfNotExists(CF_COUNTER1, CF_COUNTER1_OPTIONS);
		
		
	}
	
	public void createCFIfNotExists(ColumnFamily<?, ?> CF) throws ConnectionException {
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(CF.getName());
        if (cfDef == null) {
        	System.out.println("create CF"+CF.getName());
        	keyspace.createColumnFamily(CF, null);
        }
	}
	
	public void createCFIfNotExists(ColumnFamily<?, ?> CF, Map<String, Object> CF_Options) throws ConnectionException {
		
		KeyspaceDefinition ksDef = keyspace.describeKeyspace();
        ColumnFamilyDefinition cfDef = ksDef.getColumnFamily(CF.getName());
        if (cfDef == null) {
        	System.out.println("create CF"+CF.getName());
        	keyspace.createColumnFamily(CF, CF_Options);
        }
	}
	
	@After
	public void shutdown() {
		context.shutdown();
	}
	
	@Test
	public void incrementCountColumns() throws Exception {
		
		List<String[]> inputs = new ArrayList<String[]>();
		String[] arr1 = {"1386520005", "1", "arr1"};
		String[] arr2 = {"1386523205", "1", "arr2"};
		String[] arr3 = {"1386525405", "1", "arr3"};
		String[] arr4 = {"1386525405", "1", "arr4"};
		String[] arr5 = {"1386520005", "1", "arr5"};
		
		inputs.add(arr1);
		inputs.add(arr2);
		inputs.add(arr3);
		inputs.add(arr4);
		inputs.add(arr5);
		
		System.out.println("start incrementCountColumns");
        Map<String, MutationBatch> mutations = new HashMap<String, MutationBatch>();
        for (String[] input : inputs) {
            String ks = config.getKeySpace();            
            MutationBatch mutation = mutations.get(ks);
            if(mutation == null) {
                mutation = keyspace.prepareMutationBatch();
                mutations.put(ks, mutation);
            }
            String rowKey = input[0];
            long incrementAmount = Long.parseLong(input[1]);
            //K rowKey = tupleMapper.mapToRowKey(input);
            //long incrementAmount = tupleMapper.mapToIncrementAmount(input);
            //ColumnFamily<K, C> columnFamily = new ColumnFamily<K, C>(columnFamilyName,
            //        (Serializer<K>) serializerFor(tupleMapper.getKeyClass()),
            //        (Serializer<C>) serializerFor(tupleMapper.getColumnNameClass()));
            //for (C columnName : tupleMapper.mapToColumnList(input)) {
            String columnName = input[2];
            	String test = "{columnName: "+columnName+", incrementAmount: "+incrementAmount+"}";
            	System.out.println(test);
            	Files.append(test, new File("column.out"), Charset.forName("UTF-8"));
                mutation.withRow(CF_COUNTER1, rowKey).incrementCounterColumn(columnName, incrementAmount);
            //}
        }
        for(String key : mutations.keySet()) {
            mutations.get(key).execute();
        }
    }
	
	
	@Test
	public void incrementTest() throws ConnectionException {
		/*
		MutationBatch m = keyspace.prepareMutationBatch();
		m.withRow(CF_COUNTER1, 1386520005L).incrementCounterColumn("arr1", 1);
		m.execute();
		*/
		keyspace.prepareColumnMutation(CF_COUNTER1, "1386520005", "CounterColumn1")
	    .incrementCounterColumn(1)
	    .execute();
		
	}
	
	
	
	@Test
	public void createRowKey() {
		MutationBatch m = keyspace.prepareMutationBatch();

		String rowKey = "1386520005";

		m.withRow(CF_COUNTER1, rowKey)
		    .putColumn("arr1", 1);

		try {
		    m.execute();
		} catch (ConnectionException e) {
		    
		}
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
		System.out.println("drop columnFamily "+LogConstants.CASSANDRA_MINUTE_COUNT_CF_NAME+".");
		keyspace.dropColumnFamily("LoggingMinuteCounterSuper");
	}
	
	@Test
	public void createCF() throws ConnectionException {
		System.out.println("create columnFamily "+LogConstants.CASSANDRA_MINUTE_COUNT_CF_NAME+".");
		keyspace.createColumnFamily(CF_COUNTER1, CF_COUNTER1_OPTIONS);
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
            currentBatch.withRow(CF_COUNTER1, rowKey).delete();
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
		    rows = keyspace.prepareQuery(CF_COUNTER1)
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
			keyspace.prepareQuery(CF_COUNTER1)
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
