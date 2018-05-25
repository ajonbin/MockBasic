public class DatabaseManager {
	public DatabaseManager(){
		System.out.println("Real DatabaseManager()");
	}

	public void connectToDb(String database){
		System.out.println("Real connectToDb()");
	}

	public boolean isTableExist(String tableName){
		System.out.println("Real isTableExist()");
		return true;
	}

	public void excuteCmd(String cmd){
		System.out.println("Real excuteCmd()");
	}
}
