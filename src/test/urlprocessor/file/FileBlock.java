package test.urlprocessor.file;

import java.io.File;

/**
 * Represents a File Block. It is combination of File and the Block of content in the file.
 * @author prathameshjagtap
 *
 */
public class FileBlock {

	private File file;
	private int blockNumber;
	
	public FileBlock(File file, int blockNumber) {
		super();
		this.file = file;
		this.blockNumber = blockNumber;
	}
	
	public File getFile() {
		return file;
	}
	
	public void setFile(File file) {
		this.file = file;
	}
	
	public int getBlockNumber() {
		return blockNumber;
	}

	public void setBlockNumber(int blockNumber) {
		this.blockNumber = blockNumber;
	}
	
}
