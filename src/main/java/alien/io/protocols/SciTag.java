package alien.io.protocols;

/**
 * @author costing
 * @since 2024-08-03
 * 
 * @see <a href="https://www.scitags.org/api.json">SciTags JSON</a>
 */
public enum SciTag {
	/**
	 * Default value if not indicated otherwise
	 */
	DEFAULT(1),

	/**
	 * perfSONAR, set by the tool and not the code, just to be aware of it
	 */
	PERFSONAR(2),

	/**
	 * Xrootd caches, set by the tools if ever used
	 */
	CACHE(3),

	/**
	 * Pure data challenge activities
	 */
	DATACHALLENGE(4),

	/**
	 * Data Consolidation (not yet used)
	 */
	DATA_CONSOLIDATION(8),

	/**
	 * Should be set by the JA/JW
	 */
	JOB_PREPARATION(9),

	/**
	 * Set by envelope requests over WebSockets, assuming this is C++ code
	 */
	DATA_ACCESS(10),

	/**
	 * Envelopes issued for CCDB objects should have this set, irrespective of the code path that led to this access
	 */
	CALIBRATION_DATA_ACCESS(11),

	/**
	 * JA/JW writes
	 */
	JOB_OUTPUT(12),

	/**
	 * All transfers, 3rd party or otherwise
	 */
	DATA_REPLICATION(13),

	/**
	 * Not sure we can tell the CLI apart from other operations
	 */
	CLI_DOWNLOAD(14),

	/**
	 * Same, how to identify them?
	 */
	CLI_UPLOAD(15),

	/**
	 * alimonitor tests or testSE commands
	 */
	FUNCTIONAL_TESTS(16);

	private final int tag;

	private SciTag(final int tag) {
		this.tag = tag;
	}

	/**
	 * @return SciTag, combining the experiment ID (5<<6 for ALICE) with the activity value
	 */
	public int getTag() {
		return (5 << 6) + tag;
	}
}
