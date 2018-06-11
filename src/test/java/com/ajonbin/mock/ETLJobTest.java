package com.ajonbin.mock;

import com.ajonbin.mock.DatabaseManager;
import com.ajonbin.mock.ETLJob;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;

import static junit.framework.TestCase.fail;
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
	public  void testWithMock() throws Exception {
		//Mock Configurations

		//connectToDb
		doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Throwable {
				System.out.println("Mock connectToDb() with: " + invocation.getArgument(0));
				return null;
			}
		}).when(databaseManagerMock).connectToHive(any(String.class));

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