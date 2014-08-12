package ca.dealsaccess.holt.mysql;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;

import ca.dealsaccess.holt.common.AbstractConfig.ConfigException;
import ca.dealsaccess.holt.common.MySQLConfig;
import ca.dealsaccess.holt.util.GsonUtils;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class MySQLClient {

	DataSource mysqlDS;

	MySQLConfig config;

	public static DataSource getMySQLDataSource(MySQLConfig config) {
		MysqlDataSource mysqlDS = new MysqlDataSource();
		mysqlDS.setURL(config.getJDBC() + config.getHost() + ":" + config.getPort() + "/" + config.getDatabase());
		mysqlDS.setUser(config.getUsername());
		mysqlDS.setPassword(config.getPassword());
		return mysqlDS;
	}

	public MySQLClient() throws ConfigException {

		config = new MySQLConfig(true);
		config.parseProperties();
		mysqlDS = getMySQLDataSource(config);
	}

	public MySQLConfig getConfig() {
		return config;
	}

	public DataSource getMysqlDS() {
		return mysqlDS;
	}

	public void createLogStatsTableIfnotExists() {
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

	}
}
