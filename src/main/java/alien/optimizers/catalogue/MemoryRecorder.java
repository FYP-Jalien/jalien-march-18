package alien.optimizers.catalogue;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import alien.config.ConfigUtils;
import alien.optimizers.DBSyncUtils;
import alien.optimizers.Optimizer;
import lazyj.DBFunctions;

public class MemoryRecorder extends Optimizer {

	/**
	 * Logging facility
	 */
	static final Logger logger = ConfigUtils.getLogger(LTables.class.getCanonicalName());

	private final String memoryReporterUrl = "http://pcalimonitor4.cern.ch:8080/agent/memoryreporter/";

	private final int SLEEP_PERIOD = 600 * 1000;
	private final long updateMaxInterval = 6 * 60 * 60 * 1000; // 6 hours

	@Override
	public void run() {
		this.setSleepPeriod(SLEEP_PERIOD); // 10 minutes
		final int frequency = SLEEP_PERIOD; // 10 minutes

		logger.log(Level.INFO, "MemoryReporter optimizer starts");

		DBSyncUtils.checkLdapSyncTable();
		boolean updated = false;
		try (DBFunctions db = ConfigUtils.getDB("processes"); DBFunctions db1 = ConfigUtils.getDB("processes");) {
			if (db == null || db1 == null) {
				logger.log(Level.INFO, "MemoryRecorder could not get processes DB!");
				return;
			}
			while (true) {
				updated = DBSyncUtils.updatePeriodic(frequency, MemoryRecorder.class.getCanonicalName(), this);
				if (updated) {
					int ind = 0;
					long ts = System.currentTimeMillis() - updateMaxInterval;
					String q = "select queueId, resubmissionCounter,statusId,preemptionTs,systemKillTs from oom_preemptions where MLSync = 0 and (preemptionTs>" + ts + " or systemKillTs>" + ts + ")";
					if (!db.query(q)) {
						logger.log(Level.WARNING, "Error getting data from oom_preemptions");
					}
					else {
						while (db.moveNext()) {
							ind += 1;
							long queueId = db.getl("queueId");
							int resubmissionCounter = db.geti("resubmissionCounter");
							int statusId = db.geti("statusId");
							long preemptionTs = db.getl("preemptionTs");
							long systemKillTs = db.getl("systemKillTs");
							long lastRecordTs = 0;
							if (preemptionTs > 0) {
								if (systemKillTs > 0) {
									lastRecordTs = Math.max(preemptionTs, systemKillTs);
								} else {
									lastRecordTs = preemptionTs;
								}
							} else {
								if (systemKillTs > 0) {
									lastRecordTs = systemKillTs;
								}
							}
							if (queueId > 0l && statusId != 0 && lastRecordTs > 0) {
								logger.log(Level.FINE, "Recording last pss,swappss for queueId " + queueId + " and resubmissionCOunt " + resubmissionCounter + " and statusId=" + statusId + " at time " + lastRecordTs);
								JSONObject lastMemoryReports = getLastMemoryReport(queueId, lastRecordTs);

								Double pss = Double.valueOf(0d), swappss = Double.valueOf(0d);
								if (lastMemoryReports != null && lastMemoryReports.keySet().size() > 0) {
									for (Object limit_key : lastMemoryReports.keySet()) {
										String key_str = (String) limit_key;
										if (key_str.startsWith("pss")) {
											pss = (Double) lastMemoryReports.get(limit_key);
										}
										if (key_str.startsWith("swappss")) {
											swappss = (Double) lastMemoryReports.get(limit_key);
										}
									}

									String w = "update oom_preemptions set lastMLPss=" + (pss.doubleValue() / 1024) + ", lastMLSwapPss=" + (swappss.doubleValue() / 1024) + ", MLSync=1 where queueId="
											+ queueId + " and resubmissionCounter=" + resubmissionCounter + " and statusId is not null";
									if (!db1.query(w)) {
										logger.log(Level.WARNING, "Error updating last memory reports from ML in oom_preemptions");
									}
								}
							}
						}
						String log = "ML Sync " + ind + " jobs";
						DBSyncUtils.registerLog(MemoryRecorder.class.getCanonicalName(), log);
					}
				}
				try {
					logger.log(Level.INFO, "MemoryRecorder sleeps " + this.getSleepPeriod());
					sleep(this.getSleepPeriod());
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private JSONObject getLastMemoryReport(long queueId, long lastRecordTs) {
		try {
			final URL url = new URL(memoryReporterUrl + "getJobLastReport.jsp?queueId=" + queueId + "&lastRecordTs=" + lastRecordTs);
			return makeRequest(url, queueId, logger);
		}
		catch (final IOException e) {
			logger.log(Level.SEVERE, "Failed to get last memory report for queueId" + queueId, e);
		}
		return null;
	}

	protected static JSONObject makeRequest(URL url, long queueId, Logger logg) {
		try {
			JSONParser jsonParser = new JSONParser();
			logg.log(Level.FINE, "Making HTTP call to " + url + " from " + queueId);
			final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
			conn.setConnectTimeout(5000);
			conn.setReadTimeout(120000);

			try (InputStream inputStream = conn.getInputStream()) {
				byte[] buffer = inputStream.readAllBytes();
				String output = new String(buffer, StandardCharsets.UTF_8);
				if (!output.isBlank()) {
					return (JSONObject) jsonParser.parse(output);
				}
			}
			catch (final ParseException e) {
				logg.log(Level.SEVERE, "Failed to parse AliMonitor response for queueId " + queueId, e);
			}
		}
		catch (final IOException e) {
			logg.log(Level.SEVERE, "IO Error in calling the url " + url + " for queueId " + queueId, e);
		}

		return null;
	}

}
