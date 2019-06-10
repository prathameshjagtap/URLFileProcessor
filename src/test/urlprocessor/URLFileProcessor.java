package test.urlprocessor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import test.urlprocessor.file.AsyncFileReader;
import test.urlprocessor.file.FileManager;
import test.urlprocessor.http.HttpClientManager;
import test.urlprocessor.http.HttpGetBlockProcessor;

public class URLFileProcessor {

	private final int NO_OF_IO_TASK_PER_CORE = 50;
	private final int NO_OF_CORES;
	private final int NO_OF_BLOCK_IN_QUEUE = 10000;
	private final String FILE_DIRECTORY;
	private final BlockingQueue<List<String>> workQueue;
	
	private ProgressReport progress;
	private ExecutorService threadPool;
	private FileManager fileManager;
	
	public URLFileProcessor(int cores, String directory) throws Exception {
		
		NO_OF_CORES = cores;
		FILE_DIRECTORY = directory;
		
		progress = new ProgressReport(2);
		threadPool = Executors.newFixedThreadPool((NO_OF_CORES * NO_OF_IO_TASK_PER_CORE) + NO_OF_CORES);

		HttpClientManager.getInstance().tune(NO_OF_CORES * NO_OF_IO_TASK_PER_CORE);
		
		fileManager = new FileManager(FILE_DIRECTORY);
		workQueue = new LinkedBlockingDeque<>(NO_OF_BLOCK_IN_QUEUE);
		
	}
	
	private void process() {
		
		long startTime = System.currentTimeMillis();
		startProgressTracker();
		List<Future<Boolean>> fileReadFutures = startFileReaderThreads();
		List<Future<Boolean>> urlReadFutures = startProcessorThreads();
		
		waitForFileReaderThreadsToComplete(fileReadFutures);
		waitForProcessorThreadsToComplete(urlReadFutures);
		
		long endTime = System.currentTimeMillis();
		
		progress.printStatus();
		System.out.println();
		System.out.println("TIME TAKEN: " + ((endTime - startTime) / 1000 ) + " secs");
		
		threadPool.shutdown();
		try {
			threadPool.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void waitForProcessorThreadsToComplete(List<Future<Boolean>> urlReadFutures) {
		for (Future<Boolean> future : urlReadFutures) {
			try{
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				System.out.println("ERROR IN A THREAD");
				e.printStackTrace();
			}
		}
		progress.markComplete(1);

	}

	private List<Future<Boolean>> startProcessorThreads() {
		List<Future<Boolean>> urlReadFutures = new ArrayList<>();
		for (int i = 0; i < NO_OF_CORES * NO_OF_IO_TASK_PER_CORE; i++) {
			urlReadFutures.add(threadPool.submit(new HttpGetBlockProcessor(workQueue, progress, 1)));
		}

		return urlReadFutures;
	}

	private void waitForFileReaderThreadsToComplete(List<Future<Boolean>> fileReadFutures) {
		for (Future<Boolean> future : fileReadFutures) {
			try {
				future.get();
			} catch (InterruptedException | ExecutionException e) {
				System.out.println("ERROR IN A THREAD");
				e.printStackTrace();
			}
		}
		progress.markComplete(0);
		
		System.out.println("FILE READ COMPLETE");
	}

	private List<Future<Boolean>> startFileReaderThreads() {
		List<Future<Boolean>> fileReadFutures = new ArrayList<>();
		for (int i = 0; i < NO_OF_CORES; i++) {
			fileReadFutures.add(threadPool.submit(new AsyncFileReader(fileManager, workQueue)));
		}
		return fileReadFutures;
	}

	private void startProgressTracker() {
		threadPool.submit(new Runnable() {
			
			@Override
			public void run() {
				while (!progress.isStepComplete(1)) {
					progress.printStatus();
					try { Thread.sleep(5 * 1000); } catch (InterruptedException e) { e.printStackTrace(); }
				}
			}
		});
	}

	public static void main(String[] args) throws Exception {
		int cores;
		String dir;
		if (args.length == 2) {
			cores = Integer.parseInt(args[0]);
			dir = args[1];
		} else {
			cores = Runtime.getRuntime().availableProcessors();
			dir = "inputData.zip";
		}
		
		URLFileProcessor fileProcessor = new URLFileProcessor(cores, dir);
		fileProcessor.process();
	}
	
}
