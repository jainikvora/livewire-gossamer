package poke.server.election.raft.log;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.core.Mgmt.LogEntry;;

/**
 * In memory log of LogEntry storing the logs in a TreeMap with key as monotonically increasing number.
 * 
 * Implements Loggable interface with additional methods of getting entries from a start index to end 
 * of the log.
 * 
 * Commit Index will be maintained by Raft to indicate the last index of log committed across majority 
 * of nodes
 * 
 * If the log reaches size of a threshold value, it will be stored in file as it is. And retrieving,
 * files would be parsed based on given start index.
 * 
 * TODO Logger implementation, Changes needed as raft gets implemented
 * 
 * @author jainikvora
 *  
 */
public class BufferedLog implements Loggable{
	//protected static Logger logger = LoggerFactory.getLogger("logging");
	
	public TreeMap<Long, LogEntry> log;
	private Long commitIndex;
	private Long lastApplied;
	private int thresholdSize = 65536;
	private String logStoreDir = "./resources/files";
	
	public BufferedLog() {
		log = new TreeMap<Long,LogEntry>();
		commitIndex = (long) 0;
		lastApplied = (long) 0;
	}
	
	/**
	 * returns size of the in-memory log
	 * @return long size
	 */
	@Override
	public long size() {
		return log.size();
	}

	/**
	 * Append a single entry at the end of the log. Checks if the log size
	 * reached threshold after appending. If yes, store the log in new file.
	 * 
	 * @param LogEntry entry
	 * @return long index
	 */
	@Override
	public long appendEntry(LogEntry entry) {
		
		long index = log.isEmpty() ? 1 : log.lastKey() + 1;
		log.put(index, entry);
		
		if(isSegmentLimitReached()) {
			storeLogSegment();
		}
		
		return index;
	}
	
	/**
	 * Append a list of entries at the end of the log. Checks if the log size
	 * reached threshold after appending each entry. If yes, store the log in 
	 * a new file.
	 * 
	 * @param LogEntry[] entries
	 * @return Long index - last index
	 */
	public long appendEntry(LogEntry[] entries) {
		long index = log.isEmpty() ? 0 : log.lastKey();
		
		for(LogEntry logEntry: entries) {
			log.put(++index, logEntry);
			
			if(isSegmentLimitReached()) {
				storeLogSegment();
			}
		}
		
		return index;
	}

	@Override
	public long firstIndex() {
		return log.firstKey();
	}

	@Override
	public long lastIndex() {
		return size() > 0 ? log.lastKey() : 0;
	}

	/**
	 * Retrieves a single log entry present at provided index. If index is less
	 * than start index of in-memory log, the entry would be retrieved from the
	 * file
	 * 
	 * @param Long index
	 * @return LogEntry
	 */
	@Override
	public LogEntry getEntry(long index) {
		if(index < 1)
			return null;
		
		if(index < firstIndex()) {
			TreeMap<Long, LogEntry> fileLogMap = retrieveLogSegment(index, index+1);
			return fileLogMap.get(index);
		} else {
			return log.get(index);
		}
	}
	
	/**
	 * Retrieves all the entries starting from given start index until the end of 
	 * the log. If startIndex < firstIndex of the in-memory log, file search is done
	 * and the required entries are retrieved.
	 * 
	 * @param startIndex
	 * @return LogEntry[] - array of log entries in order from startIndex to lastIndex
	 */
	public LogEntry[] getEntries(long startIndex) {
		if(startIndex < 1)
			return null;
		
		int size = (int) (lastIndex() - startIndex)+1;
		LogEntry[] entries = new LogEntry[size];
		int i = 0;
		
		if(startIndex < firstIndex()) {
			
			TreeMap<Long, LogEntry> fileLogMap = retrieveLogSegment(startIndex, firstIndex() - 1);

			while(i < fileLogMap.size()) {
				entries[i++] = fileLogMap.get(startIndex++);
			}
		}
		
		while(i < size) {
			entries[i++] = log.get(startIndex++);
		}
		
		return entries;
	}
	
	/**
	 * Remove all the entries from log starting with startIndex to
	 * lastIndex
	 * @param startIndex
	 */
	public void removeEntry(long startIndex) {
		if(startIndex < 1)
			return;
		
		if(startIndex < firstIndex()) {
			log.clear();
			log.putAll(removeLogSegment(startIndex));
		}
		for(long i = startIndex; i <= lastIndex(); i++)
			log.remove(i);		
	}
	
	/**
	 * Increment the commit index by given integer.
	 * @param i
	 * @return long - updated commitIndex
	 */
	public long incCommitIndex(int i) {
		commitIndex += i;
		return commitIndex;
	}
	
	/**
	 * Returns current value of commitIndex
	 * @return long commitIndex
	 */
	public long getCommitIndex() {
		return commitIndex;
	}
	
	/**
	 * Set commitIndex to passed value
	 * @param commitIndex
	 */
	public void setCommitIndex(Long commitIndex) {
		this.commitIndex = commitIndex;
	}
	
	// storing logs in file when size exceeds the limit
	
	/**
	 * Checks if the map size has reached the threshold value.
	 * @return boolean 
	 */
	public boolean isSegmentLimitReached() {
		if(size() != 1 && (size()-1) % thresholdSize == 0) 
			return true;
		return false;
	}
	
	/**
	 * Stores the log present in memory to a file.
	 * File name convention - RaftLog_[lastIndex of the log to be stored in the file]
	 * 
	 */
	public void storeLogSegment() {
		LogEntry lastEntry = log.get(lastIndex());
		
		long lastIndex = lastIndex() - 1;
		String fileName = "RaftLog_" + lastIndex;
		
		try {
			log.remove(lastIndex());
			
			File file = new File(logStoreDir,fileName);
			
			if(!file.exists()) {
				file.createNewFile(); 
			}
			
			ObjectOutputStream o = new ObjectOutputStream(new FileOutputStream(file));
			o.writeObject(log);
			o.close();
			
			log.clear();
			log.put(lastIndex+1, lastEntry);
			
		} catch(IOException e) {
			log.put(lastIndex+1, lastEntry);
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		}
	}
	
	/**
	 * Retrieves log segment from single/multiple files starting from startIndex to 
	 * endIndex
	 * 
	 * @param startIndex
	 * @param endIndex
	 * @return TreeMap of requested log
	 * 
	 */
	public TreeMap<Long, LogEntry> retrieveLogSegment(long startIndex, long endIndex) {
		TreeMap<Long, LogEntry> bufferMap = new TreeMap<Long, LogEntry>();
		TreeMap<Long, LogEntry> tmpMap = new TreeMap<Long, LogEntry>();
		
		File dir = new File(logStoreDir);
		File[] logFiles = dir.listFiles();
		
		try{
			for(File file: logFiles) {
				int fileLastIndex = Integer.parseInt(file.getName().split("_")[1]);
				int fileStartIndex = fileLastIndex - thresholdSize + 1;
				
				if(fileLastIndex >= startIndex) {
					ObjectInputStream oi = new ObjectInputStream(new FileInputStream(file));
					if(fileStartIndex < startIndex) {
						tmpMap = (TreeMap<Long, LogEntry>)oi.readObject();
						
						while(startIndex <= fileLastIndex) {
							bufferMap.put(startIndex, tmpMap.get(startIndex));
							startIndex++;
						}
						
						tmpMap.clear();
					} else {
						bufferMap.putAll((TreeMap<Long, LogEntry>)oi.readObject());
						
					}
					oi.close();
				} else {
					continue;
				}
			}
			
		} catch(IOException e) {
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		}
		
		return bufferMap;
	}
	
	public TreeMap<Long, LogEntry> removeLogSegment(long startIndex) {
		File dir = new File(logStoreDir);
		File[] logFiles = dir.listFiles();
		TreeMap<Long, LogEntry> tmpMap = new TreeMap<Long, LogEntry>();
		
		try{
			for(File file: logFiles) {
				int fileLastIndex = Integer.parseInt(file.getName().split("_")[1]);
				int fileStartIndex = fileLastIndex - thresholdSize + 1;
				
				if(fileLastIndex >= startIndex) {
					ObjectInputStream oi = new ObjectInputStream(new FileInputStream(file));
					if(fileStartIndex <= startIndex) {
						tmpMap = (TreeMap<Long, LogEntry>)oi.readObject();
						file.delete();
					} else {
						file.delete();
					}
					oi.close();
				} else {
					continue;
				}
			}
			
		} catch(IOException e) {
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		} catch (ClassNotFoundException e) {
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		}
		return tmpMap;
	}
	
	public Long getLastApplied() {
		return lastApplied;
	}

	public void setLastApplied(Long lastApplied) {
		this.lastApplied = lastApplied;
	}
	
	// functions written to test file operations
	
	public void testFileCreation() {
		try {
			String testString = "Hello";
			File file = new File("./fresources/iles","RaftLog_9");
			if(!file.exists()) {
				file.createNewFile(); 
			}
			FileOutputStream f = new FileOutputStream(file);
			ObjectOutputStream o = new ObjectOutputStream(f);
			o.writeObject(testString);
			o.close();
			
			ObjectInputStream oi = new ObjectInputStream(new FileInputStream("./files/test.txt"));
			String inputString = (String)oi.readObject();
			System.out.println(inputString);
			oi.close();
		} catch(Exception e) {
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		}
	}
	
	public void testFileParsing() {
		try {
			File parent = new File("./resources/files");
			File[] files = parent.listFiles();
			for(File file: files) {
				String fileName = file.getName();
				int lastIndex = Integer.parseInt(fileName.split("_")[1]);
				System.out.println(lastIndex);
			}
			
		} catch(Exception e) {
			//logger.error(e.getMessage());
			System.out.println(e.getMessage());
		}
	}
}
