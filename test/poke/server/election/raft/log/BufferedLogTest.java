package poke.server.election.raft.log;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * JUnit test cases for BufferedLog
 * @author jainik
 *
 */
public class BufferedLogTest {


/*	@Test
	public void testTestFileCreation() {
		BufferedLog bl = new BufferedLog();
		bl.testFileCreation();
	}*/
	
	@Test
	public void testTestFileParsing() {
		BufferedLog bl = new BufferedLog();
		bl.testFileParsing();
	}
	
/*	@Test
	public void testLogFileStorage() {
		File file = new File("./resources/files");
		File[] files = file.listFiles();
		for(File f : files) {
			f.delete();
		}
		
		BufferedLog bl = new BufferedLog();
		for(int i = 1; i < 57 ; i++) {
			bl.appendEntry("Test"+ i);
		}
		
		String[] result = bl.getEntries(52);
		//System.out.println(result.length);
		for(int i = 0; i < result.length ; i++) {
			System.out.println(result[i]);
		}
	}*/

}
