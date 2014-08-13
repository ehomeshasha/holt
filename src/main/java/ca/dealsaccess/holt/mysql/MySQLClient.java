package ca.dealsaccess.holt.mysql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.MySQLConfig;
import ca.dealsaccess.holt.log.LogConstants;
import ca.dealsaccess.holt.util.GsonUtils;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class MySQLClient {

	public static final String LOGGING = "Logging";

	public static final String COUNTER = "Counter";
	
	public static final String IP_TABLE = "IP_TABLE";
	
	private DataSource mysqlDS;

	private MySQLConfig config;
	
	private StringBuilder sb = new StringBuilder();

	private List<String> tableList;
	
	public static DataSource getMySQLDataSource(MySQLConfig config) {
		MysqlDataSource mysqlDS = new MysqlDataSource();
		mysqlDS.setURL(config.getJDBC() + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase());
		mysqlDS.setUser(config.getUsername());
		mysqlDS.setPassword(config.getPassword());
		return mysqlDS;
	}

	public MySQLClient() throws ConfigException, SQLException {

		config = new MySQLConfig(true);
		config.parseProperties();
		mysqlDS = getMySQLDataSource(config);
		//tableList = fetchTableList();
		//GsonUtils.print(tableList);
		
		
		
	}

	private List<String> fetchTableList() throws SQLException {
		ResultSetHandler<List<String>> h = new ResultSetHandler<List<String>>() {
			public List<String> handle(ResultSet rs) throws SQLException {
				List<String> result = new ArrayList<String>();
				while (rs.next()) {
					result.add((String) rs.getObject(1));
				}
				return result;
			}
		};
		QueryRunner run = new QueryRunner(mysqlDS);
		List<String> result = run.query("SHOW TABLES", h);
		return result;
	}

	public MySQLConfig getConfig() {
		return config;
	}

	public DataSource getMysqlDS() {
		return mysqlDS;
	}

	public void createLogStatsTableIfnotExists() throws SQLException {
		tableList = fetchTableList();
		createAllCounterTableIfNotExist();
		createAllIPTableIfNotExist();
		
		
		
		
		/*
		ResultSetHandler<Object[]> h = new ResultSetHandler<Object[]>() {
			public Object[] handle(ResultSet rs) throws SQLException {
				if (!rs.next()) {
					return null;
				}

				ResultSetMetaData meta = rs.getMetaData();
				int cols = meta.getColumnCount();
				Object[] result = new Object[cols];

				for (int i = 0; i < cols; i++) {
					result[i] = rs.getObject(i + 1);
				}

				return result;
			}
		};

		// Create a QueryRunner that will use connections from
		// the given DataSource
		QueryRunner run = new QueryRunner(mysqlDS);

		// Execute the query and get the results back from the handler
		try {
			Object[] result = run.query("SELECT * FROM dw_groups WHERE 1 LIMIT 5", h);
			GsonUtils.print(result);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	*/
	}

	private void createAllIPTableIfNotExist() {
		// TODO Auto-generated method stub
		
	}

	private void createAllCounterTableIfNotExist() {
		for(int i=0;i<LogConstants.LOG_DURATION.length;i++) {
			String tName = sb.append(LOGGING).append(LogConstants.LOG_DURATION[i]).append(COUNTER).toString();
			createCounterTableIfNotExists(tName);
			sb.setLength(0);
		}
		
	}

	private void createCounterTableIfNotExists(String tName) {
		if(tableList == null) return;
		if(!tableList.contains(tName)) {
			
		}
		
	}

	private void createCounterCFIfNotExists(String tName) {
		
		
	}
}
