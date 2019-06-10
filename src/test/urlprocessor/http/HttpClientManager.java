package test.urlprocessor.http;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Map.Entry.comparingByKey;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import static java.util.stream.Collectors.*;

import org.apache.http.HttpHost;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * A Singleton class that manages Http Connection pool for bulk Http Requests.
 * This class continuously tunes the connection pool to allot more connections to
 * most commonly used URL.
 * @author prathameshjagtap
 *
 */
public class HttpClientManager {

	private static HttpClientManager manager;
	private static ReentrantLock lock;
	
	private static Map<String, Integer> stats;
	private static ReentrantLock statsLock;

	PoolingHttpClientConnectionManager connManager;
	private int maximumConnections;
	private final double PREFERRED_POOL_LIMIT = 0.8;
	
	static {
		lock = new ReentrantLock();
		statsLock = new ReentrantLock();
	}
	
	/**
	 */
	private HttpClientManager() {
		stats = new ConcurrentHashMap<>();
		connManager = new PoolingHttpClientConnectionManager();
	}
	
	/**
	 * Instantiates and return HttpClientManager instance. 
	 * @return 
	 */
	public static HttpClientManager getInstance() {
		lock.lock();
		if(manager == null){
			manager = new HttpClientManager();
			manager.tune(100);
		}
		lock.unlock();
		
		return manager;
	}
	
	public void setMaximumConnections(int maximumConnections) {
		this.maximumConnections = maximumConnections;
	}
	
	/**
	 * Tunes connection pool to allot 80% of connections to preferred hosts and
	 * 20% of connection to all other.
	 * @param maximumConnections
	 */
	public void tune(int maximumConnections) {
		setMaximumConnections(maximumConnections);
		
		connManager.setMaxTotal(maximumConnections);
		connManager.setDefaultMaxPerRoute(maximumConnections / 5);
	}
		
	/**
	 * This method takes in upcoming URLs from the client and tune the connection pool
	 * in such a way that more the requests from A host, that A host should get more connections.
	 * It is an incremental url load. It even considers history of URLs requests claimed.
	 * 
	 * For e.g. If abc.com gets 80/100 URL requests and xyz.com get 20/100 and total 
	 * preferred connection pool is of size 100, then abc.com will get 80 maximum connection
	 * and xyz.com will get 20 maximum connections.
	 * <br/>
	 * Note: Preferred pool is driven by <b>PREFERRED_POOL_LIMIT</b> constant
	 * 
	 * @param urls Upcoming URLs Requests
	 */
	public void tune(List<String> urls) {
		
		Map<String, Integer> newStats = getStats(urls);
		updateStats(newStats);
		
		List<Map.Entry<Integer,String>> reversedAndSortedStats = reverseAndSortHashMap(stats);
		
		Map<HttpHost, Integer> hostWeights = new HashMap<>();
		
		int sum = 0;
		for (int i = 0; i < reversedAndSortedStats.size() && i < 5; i++) {
			int count = reversedAndSortedStats.get(i).getKey();
			String hostPort[] = reversedAndSortedStats.get(i).getValue().split(":");
			
			HttpHost host = new HttpHost(hostPort[0], Integer.parseInt(hostPort[1]));
			
			hostWeights.put(host, count);
			sum += count; 
		}
		
		for(Map.Entry<HttpHost, Integer> hostWeight : hostWeights.entrySet()){
			double percent = ((double)hostWeight.getValue() / (double)sum) * PREFERRED_POOL_LIMIT;
			connManager.setMaxPerRoute(new HttpRoute(hostWeight.getKey()), (int)(percent * maximumConnections));
		}
	}
	
	/**
	 * Updated existing URL stats with new Stats
	 * @param newStats
	 */
	private void updateStats(Map<String, Integer> newStats){
		statsLock.lock();
		
		newStats.entrySet().stream().forEach((entry) -> {
			Integer previosStats = stats.get(entry.getKey());
			if(previosStats == null){
				stats.put(entry.getKey(), entry.getValue());
			} else {
				stats.put(entry.getKey(), previosStats + entry.getValue());
			}
		});
		
		statsLock.unlock();
	}

	/**
	 * For given list of Urls, find the count for each unique host:port pairs
	 * @param urls
	 * @return host:port pair along with it counts 
	 */
	private Map<String, Integer> getStats(List<String> urls){
		Map<String, Integer> stats = new HashMap<>();
		
		urls.stream().forEach((urlStr) -> {
			URL url;
			
			try {
				url = new URL(urlStr);
			} catch (MalformedURLException e) {
				return;
			}
			
			String hostPort = url.getHost() + ":" + url.getPort();
			
			Integer count = stats.get(hostPort);
			if (count == null) {
				count = 0;
			}
			
			count++;
			stats.put(hostPort, count);
		});
		
		return stats;
	}
	
	/**
	 * Reverse the map by making key->Value and Value->Key <br/>
	 * After reversal, sort in non-increasing order it by Key. 
	 * Sort the 
	 * @param map Map of key -> value
	 * @return Map of value -> key
	 */
	private List<Map.Entry<Integer,String>> reverseAndSortHashMap(Map<String, Integer> map) {
		Map<Integer, String> mapInversed = 
			    map.entrySet()
			       .stream()
			       .collect(toMap(
			    		   Map.Entry::getValue, 
			    		   Map.Entry::getKey));
		
		return mapInversed.entrySet().stream()
				.sorted(Collections.reverseOrder(comparingByKey()))
				.collect(toList());

	}
	
	/**
	 * Return an HttpClient with preferred host pool configuration.
	 * @return
	 */
	public CloseableHttpClient getHttpClient() {
		return HttpClients.custom().
			    setConnectionManager(connManager).build();
	}
	
}
