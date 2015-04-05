package poke.server.election.raft.log;

import static org.junit.Assert.*;

import java.util.ArrayList;

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
		//BufferedLog bl = new BufferedLog();
		//bl.testFileParsing();
		
		ArrayList<Long> list = new ArrayList<Long>();
		list.add((long)5);
		list.add((long)6);
		list.add((long)8);
		list.add((long)5);
		list.add((long)9);
		//list.addAll(new Array(){5,6,8,5,9});
		
		int occurence = 0;
		boolean majority = false;
		long N = 9;
		
		while(N > 4 && !majority) {
			for(Long index : list) {
				if(index >= N)
					occurence += 1;
			}
			if(occurence > 5/2) {
				majority = true;
			} else {
				N -= 1;
				occurence = 0;
			}
		}
		
		System.out.println(N);
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
