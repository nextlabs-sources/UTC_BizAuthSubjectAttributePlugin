package com.nextlabs.ac.helper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HSQLHelper {

	private static final Log LOG = LogFactory.getLog(HSQLHelper.class);

	private static Properties prop;

	String connectionUrl;

	String userName;

	String password;
	ConnectionManager cp;

	/*
	 * 
	 * This constructor loads the driver and assigns the required connection
	 * 
	 * variables
	 */

	public HSQLHelper(String connectionUrl, String userName, String password,
			int size) {

		this.connectionUrl = connectionUrl;

		this.userName = userName;

		this.password = password;
		cp = ConnectionManager.getInstance(connectionUrl, userName, password,
				size);
		// while

		// reading from the real properties

		try {

			Class.forName("org.hsqldb.jdbc.JDBCDriver");

		} catch (ClassNotFoundException e) {

			LOG.error("HSQLHelper loading driver error: ", e);

		}

	}

	/*
	 * 
	 * Establishes a connection with Hsql db
	 */

	public Connection openConnection() {

		Connection hsqlConnection = null;

		hsqlConnection = cp.getConnection();

		LOG.info("OPEN Connection" + hsqlConnection);
		return hsqlConnection;

	}

	/*
	 * 
	 * For testing purpose will be removed later
	 */

	public static void main(String args[]) {

		prop = PropertyLoader

		.loadProperties("/conf/UtcAuthorityService.properties");

		HSQLHelper hsqlHelper = new HSQLHelper(

		prop.getProperty("hsql_server_url"),

		prop.getProperty("hsql_user_name"),

		prop.getProperty("hsql_password"), 10);

		hsqlHelper.openConnection();

	}

	/*
	 * 
	 * This method retrieves a list of licenses based on different where
	 * 
	 * conditions like jurisdiction,classification,expirydate,isactive
	 */

	public ArrayList<String> retrieveLicenses(QueryBuilder query) {

		Connection hsqlConnection = openConnection();

		ArrayList<String> result = new ArrayList<String>();

		if (hsqlConnection != null) {

			try {

				PreparedStatement preparedStatement = hsqlConnection

				.prepareStatement(query.getPreparedQuery());

				ArrayList<String> parameters = query.getQueryParameters();

				int parameterSize = parameters.size();

				for (int parameterIndex = 0; parameterIndex < parameterSize; parameterIndex++) {
					LOG.info("parameters.get(parameterIndex)"
							+ parameters.get(parameterIndex));
					preparedStatement.setString(parameterIndex + 1,

					parameters.get(parameterIndex));

				}

				ResultSet resultSet = preparedStatement.executeQuery();

				while (resultSet.next()) {

					result.add(resultSet.getString(1));

				}

			} catch (SQLException e) {

				LOG.info("HSQLHelper retrieveLicenses() error: ", e);
				// UTC test case 5:Bug fix start

				return null;

				// UTC test case 5:Bug fix END

			} catch (Exception e) {

				LOG.error("HSQLHelper retrieveLicenses() error: ", e);
				// UTC test case 5:Bug fix start

				return null;

				// UTC test case 5:Bug fix END

			} finally {
				closeConnection(hsqlConnection);
			}
		}

		if (result.size() > 0)
			return result;
		else
			return null;

	}

	/*
	 * 
	 * Closes a connection with hsqldb
	 */

	private void closeConnection(Connection hsqlConnection) {

		cp.handoverConnection(hsqlConnection);

	}

	/*
	 * This method retrieves the data from the database
	 */
	public HashMap<String, Object> retrieveData(QueryResultHelper qrh) {

		Connection connection = openConnection();
		HashMap<String, Object> resultMap = null;
		if (connection != null) {
			Statement stmt;
			try {
				stmt = connection.createStatement();
				ResultSet result = stmt.executeQuery(qrh.getQuery());
				if (result.next()) {
					resultMap = new HashMap<String, Object>();
					for (String fieldName : qrh.getTableFieldList()) {
						Object obj = result.getObject(fieldName);
						if (obj != null)
							resultMap.put(fieldName, obj.toString());
					}
				}
			} catch (SQLException e) {
				LOG.error("HSQLHelper retrieveData() error: ", e);
				resultMap = null;
			} finally {
				closeConnection(connection);
			}

		}

		return resultMap;
	}

	public HashMap<String, Object> retrieveCountries(QueryResultHelper qrh) {

		Connection connection = openConnection();
		HashMap<String, Object> resultMap = null;
		if (connection != null) {
			Statement stmt;
			try {
				stmt = connection.createStatement();
				ResultSet result = stmt.executeQuery(qrh.getQuery());
				if (result.next()) {
					resultMap = new HashMap<String, Object>();
					Object obj = result.getObject(qrh.getTableFieldList()
							.get(0));
					Object obj1 = result.getObject(qrh.getTableFieldList().get(
							1));
					if (obj != null && obj1 != null)
						resultMap.put(obj.toString(), obj1.toString());
					while (result.next()) {

						obj = result.getObject(qrh.getTableFieldList().get(0));
						obj1 = result.getObject(qrh.getTableFieldList().get(1));
						if (obj != null && obj1 != null)
							resultMap.put(obj.toString(), obj1.toString());

					}
				}
			} catch (SQLException e) {
				LOG.error("HSQLHelper retrieveData() error: ", e);
				resultMap = null;
			} finally {
				closeConnection(connection);
			}

		}

		return resultMap;
	}

	/*
	 * 
	 * This metod gets an attribute of a user based on the attribute given by
	 * 
	 * the user
	 */

	public Object getAttributeForUser(QueryBuilder query) {
		long lCurrentTime = System.nanoTime();
		Connection hsqlConnection = openConnection();
		LOG.debug("HSQL Open Connection :Time spent: "

		+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
		Object result = null;

		if (hsqlConnection != null) {

			if (query.getPreparedQuery() != null) {

				try {

					Statement hsqlStatement = hsqlConnection.createStatement();
					lCurrentTime = System.nanoTime();
					ResultSet rs = hsqlStatement.executeQuery(query
							.getPreparedQuery());
					LOG.debug("HSQLQuery Execution :Time spent: "

					+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
					while (rs.next()) {

						result = rs.getObject(1);

					}

				} catch (SQLException e) {
					closeConnection(hsqlConnection);
					LOG.error("HSQLHelper getAttributeForUser() error: ", e);
					// UTC test case 5:Bug fix start

					return null;

					// UTC test case 5:Bug fix END
				}

			}
			lCurrentTime = System.nanoTime();
			closeConnection(hsqlConnection);
			LOG.debug("HSQL close connection :Time spent: "

			+ ((System.nanoTime() - lCurrentTime) / 1000000.00) + "ms");
		}

		return result;

	}

	/*
	 * 
	 * This method returns a set of string without duplicates
	 */

	public ArrayList<String> getGenAttributeList(QueryBuilder query) {

		Connection hsqlConnection = openConnection();
		ArrayList<String> uniqueResult = new ArrayList<String>();
		if (hsqlConnection != null) {
			if (query.getPreparedQuery() != null) {
				try {
					Statement hsqlStatement = hsqlConnection.createStatement();
					ResultSet rs = hsqlStatement.executeQuery(query
							.getPreparedQuery());
					while (rs.next()) {
						String temp = rs.getString(1);
						if (!temp.isEmpty())
							uniqueResult.add(rs.getString(1));
					}

				} catch (SQLException e) {

					LOG.error("HSQLHelper getGenAttributeList() error: ", e);

					closeConnection(hsqlConnection);
					// UTC test case 5:Bug fix start
					return null;
					// UTC test case 5:Bug fix end

				} finally {
					closeConnection(hsqlConnection);
				}

			}

		}
		// Unit test case 5 : Bug Fix Start
		if (uniqueResult.size() > 0)
			return uniqueResult;
		else
			return null;
		// Unit test case 5 : Bug Fix End

	}

	public String getAuthId(String query) {
		String result = "";
		Connection hsqlConnection = openConnection();

		if (hsqlConnection != null) {

			try {
				Statement hsqlStatement = hsqlConnection.createStatement();
				ResultSet rs = hsqlStatement.executeQuery(query);
				while (rs.next()) {
					result = rs.getString(1);

				}

			} catch (SQLException e) {
				LOG.error("HSQLHelper getAuthId() error: ", e);

			} finally {
				closeConnection(hsqlConnection);
			}
		}

		return result;
	}

}
