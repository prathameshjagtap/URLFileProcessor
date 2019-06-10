package test.urlprocessor.file;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import test.urlprocessor.utility.UnzipFile;

/**
 * FileManager manages the files and its blocks in progress. 
 * Currently it is using simple algorithm to allot a block to client
 * every time getFileBlock is called. Also manages if the files are processed
 * and are ready to mark for completion.
 * @author prathameshjagtap
 *
 */
public class FileManager {

	private ReentrantLock lock;	
	private Map<File, Integer> files;
	
	/**
	 * Instantiates FileManager for a directory
	 * @param dirName Directory name
	 */
	public FileManager(String dirName) {
		
		lock = new ReentrantLock();
		files = new HashMap<>();
		
		File dir = new File(dirName);
		if(dir.exists()){
			if (dir.isFile()) {
				try {
					UnzipFile.unzip(dir.getAbsolutePath(), "work_area");
					File inputDir = new File("work_area/inputData");
					File destDir = new File("work_area");
					for (File file : inputDir.listFiles()) {
						UnzipFile.unGzip(file, destDir);
						file.delete();
					}
					dir = destDir;
				} catch (IOException e) {
					e.printStackTrace();
					throw new RuntimeException("Error processing file", e);
				}
			}
			
			for(File file: dir.listFiles()){
				if(file.isFile())
					files.put(file, 0);
			}

		} else {
			throw new RuntimeException("Invalid directory");
		}
	}
	
	public FileBlock getFileBlock(){
		File file;
		int block;
		
		lock.lock();
		try{
			if(files.size() == 0)
				throw new RuntimeException("No more files");

			file = files.keySet().iterator().next();
			file.length();
			block = files.get(file);
			files.put(file, block + 1);
			
		} finally {
			lock.unlock();
		}
				
		return new FileBlock(file, block);
	}
	
	public void markComplete(File file){
		lock.lock();
		files.remove(file);
		lock.unlock();		
	}
	
	public boolean hasFile(){
		return files.size() > 0;
	}
	
}
