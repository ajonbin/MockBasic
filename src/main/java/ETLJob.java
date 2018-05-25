public class ETLJob {

	protected DatabaseManager databaseManager;

	public ETLJob(DatabaseManager databaseManager){
		this.databaseManager = databaseManager;
		databaseManager.connectToDb("localhost:impala");
	}

	public void runJobs(){
		if(!databaseManager.isTableExist("table_person")){
			databaseManager.excuteCmd("create table table_person");
		}

		databaseManager.excuteCmd("select * from table_person");
	}
}
