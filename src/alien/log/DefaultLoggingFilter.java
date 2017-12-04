/**
 * DefaultLoggingFilter.java
 * @author thallybu
 * @since Dec 4, 2017
 */
package alien.log;

import java.util.HashMap;
import java.util.Map;

import java.util.logging.Filter;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


/**
 * @author lyb
 *
 * This class implements the java.util.logging.Filter interface and extends it with additional functionality to
 * report rejected/accepted log records. 
 * It's purpose is to support a 'fall back' logger which can consume any remaining records.
 */
public class DefaultLoggingFilter implements Filter {

	private transient Logger defaultLoggerRef;

	/**
	 * @param defaultLogger the default logger
	 */
	public DefaultLoggingFilter(Logger defaultLogger) {
		this.defaultLoggerRef = defaultLogger;
	}
	
	
	/**
	 * This lookup will keep a counter for each record's sequence number.
	 * The counter represents the number of loggers which rejected the associated record.
	 * Consumed records are marked with -1.
	 */
	private Map<Long, Integer> recordLookup = new HashMap<>();
	private int serviceLoggerCount = 0;

	/**
	 * All serviceLoggers must register to the default logger filter in order
	 * to keep track when to log/save a record.
	 */
	public void registerService() {
		this.serviceLoggerCount += 1;
	}
	
	/**
	 * This method is to be called by the service logger who logged the record.
	 * @param record the record
	 */
	public void notifyRecordConsumed(LogRecord record) {
		long sequenceNumber = record.getSequenceNumber();
		
		// -1 indicates consumed records
		recordLookup.put(sequenceNumber, -1);
	}
	
	/**
	 * This method is to be called by all service loggers who rejected the record.
	 * @param record the record
	 */
	public void notifyRecordRejected(LogRecord record) {
		long sequenceNumber = record.getSequenceNumber();
		// check if the record is already known
		if(recordLookup.containsKey(sequenceNumber)) {
			int currCounter = recordLookup.get(sequenceNumber);
			// check if the record has been consumed. Consumed records are marked with -1
			if(currCounter < 0) {
				return;
			} else {
				recordLookup.put(sequenceNumber, ++currCounter);
			}
			
		} else {
			// first time we see this record ( means we have one rejection for this record so far )
			recordLookup.put(sequenceNumber, 1);
		}
		
		// finally, resubmit the record to the default logger
		defaultLoggerRef.log(record);
		
	}
	
	/**
	 * This method will check the internal recordLookup. The lookup keeps track of the sequence numbers of 
	 * potential log recrods and associates them with a counter. The counter gets increased by each service logger
	 * who did not log the record. Once it reaches the amount of registered service loggers it will log the record,
	 * since it did not meet the requirements of any other logger.
	 */
	@Override
	public boolean isLoggable(LogRecord record) {
		
		if(recordLookup.containsKey(record.getSequenceNumber())) {
			return recordLookup.get(record.getSequenceNumber()) == serviceLoggerCount;
		}
		
		// unknown record
		return false;
	}

}
