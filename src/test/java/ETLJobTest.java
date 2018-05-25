import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

public class ETLJobTest {
	@Mock
	DatabaseManager databaseManagerMock;
	@Before
	public void setUp() throws Exception {
		MockitoAnnotations.initMocks(this);
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testReal(){
		DatabaseManager databaseManager = new DatabaseManager();
		ETLJob etlJob = new ETLJob(databaseManager);
		etlJob.runJobs();
	}

	@Test
	public  void testWithMock(){
		//Mock Configurations

		//connectToDb
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				System.out.println("Mock connectToDb() with: " + invocation.getArgument(0));
				return null;
			}
		}).when(databaseManagerMock).connectToDb(any(String.class));

		//excuteCmd
		//Lambda way !! Compare to upper new Answer() {...}
		doAnswer((InvocationOnMock invocation) -> {
				System.out.println("Mock excuteCmd() with: " + invocation.getArgument(0));
				return null;
		}).when(databaseManagerMock).excuteCmd(any(String.class));

		//isTableExist()
		when(databaseManagerMock.isTableExist(any(String.class))).thenAnswer(new Answer<String>() {
			@Override
			public String answer(InvocationOnMock invocation) throws Throwable {
				System.out.println("Mock isTableExist() with: " + invocation.getArgument(0));
				return null;
			}
		});


		ETLJob etlJob = new ETLJob(databaseManagerMock);
		etlJob.runJobs();

	}


}