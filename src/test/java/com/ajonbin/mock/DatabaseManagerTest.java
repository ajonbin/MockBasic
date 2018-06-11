package com.ajonbin.mock;

import org.apache.hadoop.security.UserGroupInformation;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.when;

@PrepareForTest({DriverManager.class, UserGroupInformation.class})
@RunWith(PowerMockRunner.class)
public class DatabaseManagerTest {

	protected Connection jdbcConnection;
	protected DatabaseManager dbManager;

	public final static String URL_HIVE = "jdbc:hive2://cm-hue-01.novalocal:10000/;principal=hive/cm-hue-01.novalocal@CIM.IVSG.AUTH";
	public final static String KEYTAB_FILE = "src/test/etl.keytab";

	@Before
	public void setup() throws SQLException {

		//Set up a in-memory DB
		jdbcConnection = jdbcConnection = DriverManager.getConnection("jdbc:h2:mem:test");
		PreparedStatement preparedStatement = jdbcConnection.prepareStatement("CREATE TABLE locations AS SELECT * FROM CSVREAD('src/test/resources/db/locations.csv')");
		preparedStatement.execute();

		MockitoAnnotations.initMocks(this);
	}

	@After
	public void teardown() throws SQLException {
		PreparedStatement preparedStatement = jdbcConnection.prepareStatement("DROP TABLE locations");
		preparedStatement.execute();


	}

	public void prepareConnection() throws Exception {
		PowerMockito.mockStatic(DriverManager.class);
		PowerMockito.mockStatic(UserGroupInformation.class);
		UserGroupInformation loginedUserMocked = PowerMockito.mock(UserGroupInformation.class);

		PowerMockito.when(DriverManager.getConnection(URL_HIVE)).thenReturn(jdbcConnection);
		PowerMockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(true);
		PowerMockito.doNothing().when(UserGroupInformation.class,"loginUserFromKeytab",anyString(),anyString());
		PowerMockito.when(UserGroupInformation.getLoginUser()).thenReturn(loginedUserMocked);
		PowerMockito.when(loginedUserMocked.doAs(ArgumentMatchers.<PrivilegedExceptionAction<Connection>>any()))
			.thenReturn(jdbcConnection);


		dbManager = new DatabaseManager();
		dbManager.connectToHive(URL_HIVE);

	}

	@Test
	public void testConnection() throws Exception {
			prepareConnection();
	}

	@Test
	public void testCreateInsertReadDrop() throws Exception {

		prepareConnection();
		dbManager.excuteCmd("CREATE TABLE persons(PersonID int, Name varchar(255), City varchar(255))");
		dbManager.excuteCmd("INSERT INTO persons(PersonID, Name, City) VALUES (1, 'John', 'Shanghai')");
		ResultSet resultSet = dbManager.executeQuery("select * from persons");
		while (resultSet.next()){
			assertEquals(1,resultSet.getInt("PersonID"));
			assertEquals("John",resultSet.getString("Name"));
			assertEquals("Shanghai",resultSet.getString("City"));
		}
		dbManager.excuteCmd("DROP TABLE persons");
	}
}
