package test.urlprocessor.file;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

/**
 * This worker thread is responsible to read lines from files and add work block to the 
 * Blocking Queue.
 * @author prathameshjagtap
 *
 */
public class AsyncFileReader implements Callable<Boolean>{

	FileManager fileManager;
	BlockingQueue<List<String>> workQueue;
	
	private final int BATCH_SIZE = 10000;
	byte[] contents;
	byte[] residue;

	
	public AsyncFileReader(FileManager fileManager, BlockingQueue<List<String>> workQueue) {
		this.fileManager = fileManager;
		this.workQueue = workQueue;
		this.contents = new byte[BATCH_SIZE];
		this.residue = new byte[100];
	}

	@Override
	public Boolean call() throws Exception {

		while(fileManager.hasFile()) {
			readFileBlock();
		}

		return true;
	}

	/**
	 * Read block from the fileBlock and performed adding URLs to a Blocking Queue
	 */
	private void readFileBlock() {

		FileBlock fileBlock;
		
		try {
			fileBlock = fileManager.getFileBlock();
		} catch (RuntimeException e) {
			if(e.getMessage().equals("No more files"))
				return;
			else
				throw e;
		}
		
		File file = fileBlock.getFile();
		
		try(RandomAccessFile reader = new RandomAccessFile(file, "r")) {
			
			String content = getMainContent(fileBlock, reader);
			if(content == null)
				return;
			
			StringTokenizer tokenizer = new StringTokenizer(content, "\n");
			skipFirstLineIfNotCompleteLine(fileBlock, reader, tokenizer);
			
			List<String> lines = new ArrayList<>();
			while(tokenizer.hasMoreTokens()){
				lines.add(tokenizer.nextToken());
			}
			
			updateLastLineIfNotCompleteLine(lines, fileBlock, reader, content.length());
				
			workQueue.put(lines);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Read the whole batch content using Random Access Reader
	 * @param fileBlock
	 * @param reader
	 * @return
	 * @throws IOException
	 */
	private String getMainContent(FileBlock fileBlock, RandomAccessFile reader) throws IOException{
		reader.seek(fileBlock.getBlockNumber() * BATCH_SIZE);
		
		int dataRead = reader.read(contents, 0, BATCH_SIZE);
		if(dataRead != BATCH_SIZE) {
			fileManager.markComplete(fileBlock.getFile());
			if(dataRead == -1)
				return null;
		}
		return new String(contents, 0, dataRead);
	}
	
	/**
	 * Goto beginning of batch and see if it was a new line character. 
	 * If it is new line character we dont need to skip this line.
	 * If it is not a new line character, skip this line as other batch would pick this line up using
	 * <b>updateLastLineIfNotCompleteLine</b>
	 */
	private void skipFirstLineIfNotCompleteLine(FileBlock fileBlock, RandomAccessFile reader, StringTokenizer tokenizer) throws IOException {
		if(fileBlock.getBlockNumber() > 0){
			reader.seek(fileBlock.getBlockNumber() * BATCH_SIZE - 1);
			if( '\n' != (char)reader.read()){
				tokenizer.nextToken();
			}
		}
	}

	/**
	 * Goto end of batch and see if it has a new line character. 
	 * If it is new line character we don't need to get content after this batch.
	 * If it is not a new line character, get some content from next batch in such a way that we have 
	 * a complete line. Once we have a complete line, append it to last line in list.
	 * <b>updateLastLineIfNotCompleteLine</b>
	 */
	private void updateLastLineIfNotCompleteLine(List<String> lines, FileBlock fileBlock, RandomAccessFile reader, 
			int contentLength) throws IOException {
		reader.seek(fileBlock.getBlockNumber() * BATCH_SIZE + contentLength - 1);
		if((char)reader.read() != '\n') {
			reader.seek((fileBlock.getBlockNumber() + 1) * BATCH_SIZE);
			int dataRead = reader.read(residue, 0, 100);
			if(dataRead != -1) {
				lines.set(lines.size() - 1, lines.get(lines.size() - 1) + new String(residue, 0, dataRead).split("\n")[0]);
			}
		}
	}
}
