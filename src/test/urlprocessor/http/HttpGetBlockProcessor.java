package test.urlprocessor.http;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import test.urlprocessor.ProgressReport;

/**
 * Block processor worker thread is responsible for making Http Get calls for each
 * URL in a block. This class works of a BlockingQueue to pull its work.
 * @author prathameshjagtap
 *
 */
public class HttpGetBlockProcessor implements Callable<Boolean>{

	BlockingQueue<List<String>> workQueues;
	ProgressReport progress;
	HttpClientManager httpClientManager;
	int previousStepIndex;
	
	/**
	 * 
	 * @param workQueues Blocking Queue that hold workload
	 * @param progress Instance of Progressreport to keep updating progress
	 * @param stepIndex Index of HttpGetBlockProcessor in pipeline
	 */
	public HttpGetBlockProcessor(BlockingQueue<List<String>> workQueues, ProgressReport progress, int stepIndex) {
		this.workQueues = workQueues;
		this.progress = progress;
		this.httpClientManager = HttpClientManager.getInstance();
		this.previousStepIndex = stepIndex - 1;
	}
	
	@Override
	public Boolean call() {
		
		// Check until Previous step (Block Producer) is not completed and work Queues are not empty
		while(!progress.isStepComplete(previousStepIndex) || !workQueues.isEmpty()){
			List<String> urls;
			try {
				urls = workQueues.poll(5, TimeUnit.SECONDS);
				
				if(urls == null)
					continue;
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
			
			HttpClientManager httpClientManager = HttpClientManager.getInstance();
			httpClientManager.tune(urls);
			
			while(!urls.isEmpty()){
				urls = executeUrls(urls);
			}

		}
		
		return true;
	}
	
	/**
	 * Execute Get all the URLs in list.
	 * @param urls URLs to execute
	 * @return Failed URLs List
	 */
	private List<String> executeUrls(List<String> urls) {
		int failed = 0;
		int success = 0;
		
		List<String> failedUrls = new ArrayList<>();
		
		for (String url : urls) {
			try {
				
				CloseableHttpClient httpClient = httpClientManager.getHttpClient();
				HttpGet get = new HttpGet(url);
				HttpResponse response = httpClient.execute(get);
				int code = response.getStatusLine().getStatusCode();
				
				EntityUtils.consume(response.getEntity());
				
				if(code < 400)
					success++;
				else
					failed++;
			} catch (IOException e) {
				failedUrls.add(url);
			}
		}
		
		progress.add(success, failed);
		return failedUrls;
	}

}
