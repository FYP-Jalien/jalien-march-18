package alien.taskQueue;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.security.cert.X509Certificate;

import lazyj.DBFunctions;
import lia.util.StringFactory;

/**
 * @author ron
 * @since Mar 1, 2011
 */

public class Job implements Comparable<Job>, Serializable {

	/**
	 *
	 */
	private static final long serialVersionUID = 7214453241953215533L;

	/**
	 * Job Queue ID
	 */
	public long queueId;

	/**
	 * Job Priority
	 */
	public int priority;

	/**
	 * Job exec host
	 */
	public String execHost;

	/**
	 * sent
	 */
	public long sent;

	/**
	 * split
	 */
	public long split;

	/**
	 * name - executable
	 */
	public String name;

	/**
	 * URL
	 */
	@Deprecated
	public String spyurl;

	/**
	 * executable parameters
	 */
	@Deprecated
	public String commandArg;

	/**
	 * finished
	 */
	public long finished;

	/**
	 * masterjob
	 */
	public boolean masterjob;

	/**
	 * Job status
	 */
	private JobStatus status;

	/**
	 * splitting
	 */
	public int splitting;

	/**
	 * node
	 */
	public String node;

	/**
	 * error
	 */
	public int error;

	/**
	 * current
	 */
	@Deprecated
	public String current;

	/**
	 * received
	 */
	public long received;

	/**
	 * validate
	 */
	public boolean validate;

	/**
	 * command
	 *
	 * @deprecated
	 */
	@Deprecated
	public String command;

	/**
	 * merging
	 */
	public String merging;

	/**
	 * User name
	 */
	public String user;

	/**
	 * submitHost
	 */
	public String submitHost;

	/**
	 * jdl
	 */
	public String jdl;

	/**
	 * The processed jdl
	 */
	public String processedJDL;

	/**
	 * the submitter's certificate (public)
	 *
	 * TODO : X509Certificate objects are not serializable, be careful with this field ...
	 */
	public transient X509Certificate userCertificate;

	/**
	 * path
	 */
	public String path;

	/**
	 * site
	 */
	public String site;

	/**
	 * siteId
	 */
	public int siteid;

	/**
	 * started
	 */
	public long started;

	/**
	 * expires
	 */
	public int expires;

	/**
	 * finalPrice
	 */
	public float finalPrice;

	/**
	 * effectivePriority
	 */
	@Deprecated
	public float effectivePriority;

	/**
	 * price
	 */
	public float price;

	/**
	 * si2k
	 */
	@Deprecated
	public float si2k;

	/**
	 * jobagentId
	 */
	@Deprecated
	public int jobagentId;

	/**
	 * agentid
	 */
	public int agentid;

	/**
	 * agentid
	 */
	public int resubmission;

	/**
	 * notify
	 */
	public String notify;

	/**
	 * chargeStatus
	 */
	public String chargeStatus;

	/**
	 * optimized
	 */
	public boolean optimized;

	/**
	 * mtime
	 */
	public Date mtime;

	/**
	 * Number of cores needed by this job to run
	 */
	public int cpucores;

	/**
	 * Load one row from a G*L table
	 *
	 * @param db
	 */
	Job(final DBFunctions db) {
		init(db, false);
	}

	/**
	 * Fake a job, needs to be removed one day!
	 */
	public Job() {
		// nothing
	}

	/**
	 * @param db
	 * @param loadJDL
	 */
	Job(final DBFunctions db, final boolean loadJDL) {
		init(db, loadJDL);
	}

	private void init(final DBFunctions db, final boolean loadJDL) {
		queueId = db.getl("queueId");
		priority = db.geti("priority");
		sent = db.getl("sent");
		split = db.getl("split");
		// spyurl = StringFactory.get(db.gets("spyurl"));
		// commandArg = StringFactory.get(db.gets("commandArg", null));
		finished = db.getl("finished");
		masterjob = db.getb("masterjob", false);
		splitting = db.geti("splitting");
		error = db.geti("error", -1);
		// current = StringFactory.get(db.gets("current", null));
		received = db.getl("received");
		validate = db.getb("validate", false);
		merging = StringFactory.get(db.gets("merging", null));
		siteid = db.geti("siteId");
		started = db.getl("started");
		expires = db.geti("expires");
		finalPrice = db.getf("finalPrice");
		// effectivePriority = db.getf("effectivePriority");
		price = db.getf("price");
		// si2k = db.getf("si2k");
		// jobagentId = db.geti("jobagentId");
		agentid = db.geti("agentid");
		resubmission = db.geti("resubmission");
		chargeStatus = StringFactory.get(db.gets("chargeStatus", null));
		optimized = db.getb("optimized", false);
		mtime = db.getDate("mtime", null);
		cpucores = db.geti("cpucores", 1);

		if (loadJDL) {
			jdl = db.gets("jdl");
			path = StringFactory.get(db.gets("path", null));
		}
		else {
			jdl = null;
			path = null;
		}

		if (TaskQueueUtils.dbStructure2_20) {
			status = JobStatus.getStatusByAlien(Integer.valueOf(db.geti("statusId")));
			submitHost = TaskQueueUtils.getHost(db.geti("submitHostId"));
			execHost = TaskQueueUtils.getHost(db.geti("execHostId"));
			node = TaskQueueUtils.getHost(db.geti("nodeid"));
			site = StringFactory.get(TaskQueueUtils.getSiteName(siteid));
			notify = TaskQueueUtils.getNotify(db.geti("notifyId"));
			name = command = TaskQueueUtils.getCommand(db.geti("commandId"));
			user = TaskQueueUtils.getUser(db.geti("userId"));
		}
		else {
			status = JobStatus.getStatus(db.gets("status"));
			submitHost = db.gets("submitHost");
			execHost = StringFactory.get(db.gets("execHost"));
			node = StringFactory.get(db.gets("node", null));
			notify = StringFactory.get(db.gets("notify", null));
			name = StringFactory.get(db.gets("name", null));
			command = StringFactory.get(db.gets("command", null));

			final int idx = submitHost.indexOf('@');

			if (idx > 0) {
				user = StringFactory.get(submitHost.substring(0, idx));
				submitHost = StringFactory.get(submitHost.substring(idx + 1));
			}
			else
				submitHost = StringFactory.get(submitHost);
		}
	}

	@Override
	public int compareTo(final Job o) {
		final long diff = queueId - o.queueId;

		if (diff == 0)
			return 0;

		if (diff < 0)
			return -1;

		return 1;
	}

	@Override
	public boolean equals(final Object obj) {
		if (!(obj instanceof Job))
			return false;

		return compareTo((Job) obj) == 0;
	}

	@Override
	public int hashCode() {
		return (int) queueId;
	}

	@Override
	public String toString() {
		return "Job queueId\t\t: " + queueId + "\n" + " priority\t\t: " + priority + "\n" + " execHost\t\t: " + execHost + "\n" + " sent\t\t\t: " + sent + "\n" + " split\t\t\t: " + split + "\n"
				+ " name\t\t\t: " + name + "\n" + " finished\t\t: " + finished + "\n" + " masterjob\t\t: " + masterjob
				+ "\n" + " status\t\t\t: " + status + "\n" + " splitting\t\t: " + splitting + "\n" + " node\t\t\t: " + node + "\n" + " error\t\t\t: " + error + "\n"
				+ " received\t\t: " + received + "\n" + " validate\t\t: " + validate + "\n" + " merging\t\t: " + merging + "\n" + " user\t\t\t: " + user + "\n"
				+ " submitHost\t\t: " + submitHost + "\n" + " siteId\t\t\t: " + siteid + "\n" + " started\t\t: " + started + "\n" + " expires\t\t: " + expires + "\n"
				+ " finalPrice\t\t: " + finalPrice + "\n" + " price\t\t\t: " + price + "\n"
				+ " agentid\t\t: " + agentid + "\n" + " notify\t\t\t: " + notify + "\n" + " chargeStatus\t\t: " + chargeStatus + "\n" + " optimized\t\t: " + optimized + "\n"
				+ " mtime\t\t\t: " + mtime + "\n" + " jdl\t\t\t: " + jdl + "\n path\t\t\t: " + path + "\n CPU cores\t\t: " + cpucores;
	}

	/**
	 * @return the owner of the job (AliEn account name)
	 */
	public String getOwner() {
		return user;
	}

	private static final Pattern pJDLContent = Pattern.compile("^\\s*\\[\\s*(.*)\\s*\\]\\s*$", Pattern.DOTALL | Pattern.MULTILINE);

	/**
	 * @return original JDL as in the QUEUE table
	 */
	public String getOriginalJDL() {
		return getOriginalJDL(true);
	}

	/**
	 * @param initialJDL
	 *            if <code>true</code> then the original JDL (what the user has submitted) is returned. This should be the normal case, since the alternative is to return the JDL as processed by the
	 *            central services, which is not user-friendly.
	 * @return original JDL as in the QUEUE table
	 */
	public String getOriginalJDL(final boolean initialJDL) {
		if (initialJDL) {
			if (jdl == null) {
				jdl = TaskQueueUtils.getJDL(queueId, true);

				if (jdl == null)
					return "";
			}

			return jdl;
		}

		if (processedJDL == null) {
			processedJDL = TaskQueueUtils.getJDL(queueId, false);

			if (processedJDL == null || processedJDL.length() == 0)
				processedJDL = getOriginalJDL(true);
		}

		return processedJDL;
	}

	/**
	 * @param jdlContent
	 * @return the JDL content without the enclosing [] if any
	 */
	public static String sanitizeJDL(final String jdlContent) {
		if (jdlContent == null)
			return null;

		String ret = jdlContent;

		final Matcher m = pJDLContent.matcher(ret);

		if (m.matches())
			ret = m.group(1);

		ret = ret.replaceAll("(^|\\n)\\s{1,8}", "$1");

		return ret;
	}

	/**
	 * @return the JDL contents, without the enclosing []
	 */
	public String getJDL() {
		return sanitizeJDL(getOriginalJDL());
	}

	/**
	 * @return the status, as object
	 */
	public JobStatus status() {
		return status;
	}

	/**
	 * @return status name
	 */
	public String getStatusName() {
		return status.name();
	}

	/**
	 * @return <code>true</code> if the job has finished successfully
	 */
	public boolean isDone() {
		return JobStatus.doneStates().contains(status);
	}

	/**
	 * @return <code>true</code> if the job is in a final error state
	 */
	public boolean isError() {
		return JobStatus.errorneousStates().contains(status);
	}

	/**
	 * @return <code>true</code> if the job is in a final state (either successful or failed)
	 */
	public boolean isFinalState() {
		return isDone() || isError();
	}

	/**
	 * @return <code>true</code> if the job is still active
	 */
	public boolean isActive() {
		return !isFinalState();
	}

	/**
	 * @return <code>true</code> if the job is a master job
	 */
	public boolean isMaster() {
		return masterjob;
	}

	/**
	 * @return <code>true</code> if the job has a validation flag
	 */
	public boolean usesValidation() {
		//
		// TODO:
		// grep the JDL for the validation flag, perl did the following:
		// $data->{jdl} =~ /validate\s*=\s*1/i and $validate = 1;

		try {
			final JDL j = new JDL(jdl);

			return Integer.parseInt(j.gets("validate")) == 1;
		}
		catch (@SuppressWarnings("unused") final IOException | NumberFormatException | NullPointerException e) {
			// ignore
		}

		return false;
	}
}
