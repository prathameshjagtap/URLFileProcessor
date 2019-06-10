package test.urlprocessor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Maintains status of job. This class have methods to keep track of success and 
 * failure stats.
 * @author prathameshjagtap
 *
 */
public class ProgressReport {

	ReentrantLock lock;
	private int failure;
	private int success;
	private boolean[] complete;
	
	/**
	 * Instantiates a ProgressReport with defined number of steps
	 * @param numberOfSteps number of steps in pipeline
	 */
	public ProgressReport(int numberOfSteps) {
		failure = 0;
		success = 0;
		complete = new boolean[numberOfSteps];
		lock = new ReentrantLock();
	}
	
	/**
	 * Add to existing total counts of success and failure.
	 * 
	 * @param success Number of successful lines
	 * @param failure Number of failed lines
	 */
	public void add(int success, int failure){
		lock.lock();
		this.success += success;
		this.failure += failure;
		lock.unlock();
	}
	
	/**
	 * Print current total of successes and failures
	 */
	public void printStatus() {
		System.out.println("TOTAL: " + (success + failure) + "\t\tSUCCESS: " + success + "\t\tFAILURE: " + failure);
	}
	
	/**
	 * Mark step complete
	 */
	public void markComplete(int index) {
		complete[index] = true;
	}
	
	/**
	 * Check if a given step is complete
	 * @param index 
	 * @return
	 */
	public boolean isStepComplete(int index) {
		return complete[index];
	}
}