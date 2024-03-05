package alien.taskQueue;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author ron
 * @since Mar 1, 2011
 */
public enum JobStatus {

	/**
	 * Any state (wildcard)
	 */
	ANY(-1, 0),
	/**
	 * Inserting job (10)
	 */
	INSERTING(10, 1),
	/**
	 * Updating (11)
	 */
	UPDATING(11, 23),
	/**
	 * Splitting (15)
	 */
	SPLITTING(15, 2),
	/**
	 * Split job (18)
	 */
	SPLIT(18, 3),
	/**
	 * Waiting to be picked up (20)
	 */
	WAITING(20, 5),
	/**
	 * Assigned to a site (25)
	 */
	ASSIGNED(25, 6),
	/**
	 * Job agent has started the job (40)
	 */
	STARTED(40, 7),
	/**
	 * Currently running on the WN (50)
	 */
	RUNNING(50, 10),
	/**
	 * Saving its output (60)
	 */
	SAVING(60, 11),
	/**
	 * Output saved, waiting for CS to acknowledge this (70)
	 */
	SAVED(70, 12),
	/**
	 * Files were saved, with some errors (71)
	 */
	SAVED_WARN(71, 22),
	/**
	 * Zombie (600)
	 */
	ZOMBIE(600, -15),
	/**
	 * Currently merging (701)
	 */
	MERGING(701, 13),
	/**
	 * Fantastic, job successfully completed (800)
	 */
	DONE(800, 15),
	/**
	 * Job is successfully done, but saving saw some errors (801)
	 */
	DONE_WARN(801, 16),
	/**
	 * ERROR_A (900)
	 */
	ERROR_A(900, -1),
	/**
	 * Error inserting (901)
	 */
	ERROR_I(901, -2),
	/**
	 * Error executing (over TTL, memory limits etc) (902)
	 */
	ERROR_E(902, -3),
	/**
	 * Error downloading the input files (903)
	 */
	ERROR_IB(903, -4),
	/**
	 * ERROR_S (906)
	 */
	ERROR_S(906, -7),
	/**
	 * Error saving output files (907)
	 */
	ERROR_SV(907, -9),
	/**
	 * Validation error (908)
	 */
	ERROR_V(908, -10),
	/**
	 * Cannot run the indicated validation code (909)
	 */
	ERROR_VN(909, -11),
	/**
	 * ERROR_VT (910)
	 */
	ERROR_VT(910, -16),
	/**
	 * ERROR_EW (911)
	 */
	ERROR_EW(911, -18),
	/**
	 * Waiting time expired (LPMActivity JDL tag) (912)
	 */
	ERROR_W(912, -19),
	/**
	 * Error splitting (913)
	 */
	ERROR_SPLT(913, -8),
	/**
	 * Error verifying JDL signature (914) TODO: add in AliEn too
	 */
	ERROR_VER(914, -20),
	/**
	 * Faulty
	 */
	FAULTY(950, 24),
	/**
	 * Incorrect
	 */
	INCORRECT(951, 25),
	/**
	 * Job didn't report for too long (1000)
	 */
	EXPIRED(1000, -12),
	/**
	 * Terminated (1002)
	 */
	KILLED(1002, -14);

	private final int level;

	private final int alienLevel;

	private JobStatus(final int level, final int alienLevel) {
		this.level = level;
		this.alienLevel = alienLevel;
	}

	private static Map<String, JobStatus> stringToStatus = new HashMap<>();

	private static Map<Integer, JobStatus> intToStatus = new HashMap<>();

	private static Map<Integer, JobStatus> alienToStatus = new HashMap<>();

	static {
		for (final JobStatus status : JobStatus.values()) {
			stringToStatus.put(status.name(), status);

			intToStatus.put(Integer.valueOf(status.level()), status);

			alienToStatus.put(Integer.valueOf(status.getAliEnLevel()), status);
		}

		stringToStatus.put("%", ANY);
	}

	/**
	 * @param status
	 * @return the status indicated by this name
	 */
	public static final JobStatus getStatus(final String status) {
		return stringToStatus.get(status);
	}

	/**
	 * @param level
	 * @return the status indicated by this level
	 */
	public static final JobStatus getStatus(final Integer level) {
		return intToStatus.get(level);
	}

	/**
	 * @param level
	 * @return the status object that has the indicated AliEn status ID
	 */
	public static final JobStatus getStatusByAlien(final Integer level) {
		return alienToStatus.get(level);
	}

	/**
	 * Is this job status older/more final than the other one
	 * 
	 * @param another
	 * @return true if state is larger than
	 */
	public boolean biggerThan(final JobStatus another) {
		return level > another.level;
	}

	/**
	 * Is this job status younger/less final than the other one
	 * 
	 * @param another
	 * @return level is smaller than
	 */
	public boolean smallerThan(final JobStatus another) {
		return this.level < another.level;
	}

	/**
	 * Is this job status younger/less final than or equals the other one
	 * 
	 * @param another
	 * @return level is smaller or equal with
	 */
	public boolean smallerThanEquals(final JobStatus another) {
		return this.level <= another.level;
	}

	/**
	 * Id this status a ERROR_
	 * 
	 * @return true if this is any ERROR_ state
	 */
	public boolean isERROR_() {
		return level >= 900 && level < 1000;
	}

	private static final Set<JobStatus> errorneousStates = Collections.unmodifiableSet(EnumSet.range(ERROR_A, EXPIRED));

	/**
	 * All error_*, expired and failed states
	 * 
	 * @return true if any
	 */
	public static final Set<JobStatus> errorneousStates() {
		return errorneousStates;
	}

	private static final Set<JobStatus> runningStates = Collections.unmodifiableSet(EnumSet.of(RUNNING, STARTED, SAVING));

	/**
	 * All running states
	 * 
	 * @return the set of active states
	 */
	public static final Set<JobStatus> runningStates() {
		return runningStates;
	}

	private static final Set<JobStatus> queuedStates = Collections.unmodifiableSet(EnumSet.of(ASSIGNED));

	/**
	 * All queued states
	 * 
	 * @return the set of queued states
	 */
	public static final Set<JobStatus> queuedStates() {
		return queuedStates;
	}

	private static final Set<JobStatus> finalStates = Collections.unmodifiableSet(EnumSet.range(DONE, KILLED));

	/**
	 * All queued states
	 * 
	 * @return the set of error states
	 */
	public static final Set<JobStatus> finalStates() {
		return finalStates;
	}

	private static final Set<JobStatus> doneStates = Collections.unmodifiableSet(EnumSet.range(DONE, DONE_WARN));

	/**
	 * @return done states
	 */
	public static final Set<JobStatus> doneStates() {
		return doneStates;
	}

	private static final Set<JobStatus> waitingStates = Collections.unmodifiableSet(EnumSet.of(INSERTING, EXPIRED, WAITING, ASSIGNED));

	/**
	 * All waiting states
	 * 
	 * @return waiting states
	 */
	public static final Set<JobStatus> waitingStates() {
		return waitingStates;
	}

	/**
	 * The level/index/age of this job status
	 * 
	 * @return numeric level
	 */
	public int level() {
		return level;
	}

	/**
	 * @return alien dictionary ID for this status
	 */
	public int getAliEnLevel() {
		return alienLevel;
	}

	@Override
	public String toString() {
		return name();
	}

	/**
	 * @return the SQL selection for this level only
	 */
	public String toSQL() {
		if (level == -1)
			return "%";

		return name();
	}

	/**
	 * Debug method
	 * 
	 * @param args
	 */
	public static void main(final String[] args) {
		System.err.println(alienToStatus.keySet());
	}
}
