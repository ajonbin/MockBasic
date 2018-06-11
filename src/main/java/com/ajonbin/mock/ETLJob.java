package com.ajonbin.mock;

import com.ajonbin.mock.DatabaseManager;

import java.sql.SQLException;

public class ETLJob {

	protected DatabaseManager databaseManager;

	public ETLJob(DatabaseManager databaseManager){
		this.databaseManager = databaseManager;
		try {
			databaseManager.connectToHive("jdbc:hive2://cm-hue-01.novalocal:10000/;principal=hive/cm-hue-01.novalocal@CIM.IVSG.AUTH");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void runJobs() throws SQLException {
		if(!databaseManager.isTableExist("table_person")){
			databaseManager.excuteCmd("create table table_person");
		}

		databaseManager.excuteCmd("select * from table_person");
	}
}
