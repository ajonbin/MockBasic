package com.ajonbin.mock;

import org.apache.hadoop.security.UserGroupInformation;

import java.security.PrivilegedExceptionAction;
import java.sql.*;

public class DatabaseManager {

	protected Connection hiveConnection;
	public final static String PRINCIPAL = "cm-hue-01.novalocal@CIM.IVSG.AUTH";
	public final static String KEYTAB_FILE = "src/test/resources/etl.keytab";
	public final static String HIVEDRIVER = "org.apache.hive.jdbc.HiveDriver";

	public DatabaseManager(){
		System.out.println("Real DatabaseManager()");
	}

	public void connectToHive(String hiveURL) throws Exception {
		if (hiveConnection == null || hiveConnection.isClosed()) {
			if (!UserGroupInformation.isSecurityEnabled()) {
				throw new Exception("Kerberos is not enabled!");
			}
			UserGroupInformation.loginUserFromKeytab(PRINCIPAL, KEYTAB_FILE);
			this.hiveConnection = UserGroupInformation
				.getLoginUser()
				.doAs(new PrivilegedExceptionAction<Connection>() {
					public Connection run() throws Exception {
						Class.forName(HIVEDRIVER);
						Connection hiveConn = DriverManager.getConnection(hiveURL);
						return hiveConn;
					}
				});
		}
	}

	public boolean isTableExist(String tableName){
		System.out.println("Real isTableExist()");
		return true;
	}

	public void excuteCmd(String cmd) throws SQLException {
		System.out.println("Real excuteCmd()");
		Statement hiveStmt = this.hiveConnection.createStatement();

		hiveStmt.execute(cmd);
	}
	public ResultSet executeQuery(String cmd) throws SQLException {
		System.out.println("Real executeQuery()");
		Statement hiveStmt = this.hiveConnection.createStatement();

		return hiveStmt.executeQuery(cmd);

	}
}
