package alien.shell.commands;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import alien.api.taskQueue.GetPS;
import alien.shell.ErrNo;
import alien.shell.ShellColor;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandps extends JAliEnBaseCommand {

	/**
	 * marker for -l argument
	 */
	private boolean bL = false;

	/**
	 * id of the job to get the JDL for
	 */
	private boolean getJDL = false;

	/**
	 * id of the job to get the trace for
	 */
	private boolean getTrace = false;

	private final Set<JobStatus> states = new HashSet<>();

	private final Set<String> users = new LinkedHashSet<>();

	private final Set<String> sites = new LinkedHashSet<>();

	private final Set<String> nodes = new LinkedHashSet<>();

	private final Set<Long> mjobs = new LinkedHashSet<>();

	private final Set<Long> jobid = new LinkedHashSet<>();

	private final HashMap<GetPS.PsFilters, Collection<Object>> filters = new HashMap<>();

	private String orderByKey = "queueId";

	private int limit = 0;

	private boolean bIDOnly = false;

	private static final transient Map<String, String> SHORT_TO_LONG_STATUS = new HashMap<>() {
		private static final long serialVersionUID = 1L;
		{
			put("K", JobStatus.KILLED.name());
			put("R", JobStatus.RUNNING.name());
			put("ST", JobStatus.RUNNING.name());
			put("D", JobStatus.DONE.name());
			put("DW", JobStatus.DONE_WARN.name());
			put("W", JobStatus.WAITING.name());
			put("OW", JobStatus.OVER_WAITING.name());
			put("I", JobStatus.INSERTING.name());
			put("S", JobStatus.SPLIT.name());
			put("SP", JobStatus.SPLITTING.name());
			put("SV", JobStatus.SAVING.name());
			put("SVD", JobStatus.SAVED.name());
			put("ASG", JobStatus.ASSIGNED.name());
			put("AST", JobStatus.A_STAGED.name());
			put("FM", JobStatus.FORCEMERGE.name());
			put("IDL", JobStatus.IDLE.name());
			put("INT", JobStatus.INTERACTIV.name());
			put("M", JobStatus.MERGING.name());
			put("SW", JobStatus.SAVED_WARN.name());
			put("ST", JobStatus.STAGING.name());
			put("TST", JobStatus.TO_STAGE.name());
			put("EA", JobStatus.ERROR_A.name());
			put("EE", JobStatus.ERROR_E.name());
			put("EI", JobStatus.ERROR_I.name());
			put("EIB", JobStatus.ERROR_IB.name());
			put("EM", JobStatus.ERROR_M.name());
			put("ERE", JobStatus.ERROR_RE.name());
			put("ES", JobStatus.ERROR_S.name());
			put("ESV", JobStatus.ERROR_SV.name());
			put("EV", JobStatus.ERROR_V.name());
			put("EVN", JobStatus.ERROR_VN.name());
			put("EVT", JobStatus.ERROR_VT.name());
			put("ESP", JobStatus.ERROR_SPLT.name());
			put("EW", JobStatus.ERROR_W.name());
			put("EVE", JobStatus.ERROR_VER.name());
			put("FF", JobStatus.FAILED.name());
			put("Z", JobStatus.ZOMBIE.name());
			put("XP", JobStatus.EXPIRED.name());
			put("EEW", JobStatus.ERROR_EW.name());
			put("UP", JobStatus.UPDATING.name());
			put("F", JobStatus.FAULTY.name());
			put("INC", JobStatus.INCORRECT.name());
		}
	};

	@Override
	public void run() {
		if (users.size() == 0)
			users.add(commander.getUsername());

		final List<Job> ps = commander.q_api.getPS(states, users, sites, nodes, mjobs, jobid, filters, orderByKey, limit);

		if (ps != null)
			for (final Job j : ps) {
				commander.outNextResult();
				final String owner = (j.getOwner() != null) ? j.getOwner() : "";

				final String name = (j.name != null) ? j.name.substring(j.name.lastIndexOf('/') + 1) : "";

				commander.printOut("owner", owner);
				commander.printOut("id", String.valueOf(j.queueId));
				commander.printOut("split", String.valueOf(j.split));
				commander.printOut("priority", String.valueOf(j.priority));
				commander.printOut("status", j.status().toString());
				commander.printOut("name", name);

				if (!getTrace && !getJDL) {
					final String jId = commander.bColour ? ShellColor.bold() + j.queueId + ShellColor.reset() : String.valueOf(j.queueId);

					if (bIDOnly) {
						commander.printOutln(jId);
					}
					else if (bL) {
						final String site = (j.site != null) ? j.site : "";
						final String node = (j.node != null) ? j.node : "";

						commander.printOut("site", site);
						commander.printOut("node", node);

						commander.printOutln(padLeft(String.valueOf(owner), 10) + padSpace(4) + padLeft(jId, 10) + padSpace(2) + printPriority(j.status(), j.priority) + padSpace(2)
								+ padLeft(String.valueOf(site), 38) + padSpace(2) + padLeft(String.valueOf(node), 40) + padSpace(2) + abbrvStatus(j.status()) + padSpace(2)
								+ padLeft(String.valueOf(name), 30));
					}
					else {
						commander.printOutln(padLeft(String.valueOf(owner), 10) + padSpace(1) + padLeft(jId, 10) + padSpace(2) + printPriority(j.status(), j.priority) + padSpace(2)
								+ abbrvStatus(j.status()) + padSpace(2) + padLeft(String.valueOf(name), 32));
					}
				}

				if (getTrace) {
					final String tracelog = commander.q_api.getTraceLog(j.queueId);

					if (tracelog != null && !tracelog.isBlank()) {
						if (commander.bColour)
							commander.printOutln(ShellColor.jobStateBlue() + tracelog + ShellColor.reset());
						else
							commander.printOutln(tracelog);

						commander.printOut("trace", tracelog);
					}
					else
						commander.setReturnCode(ErrNo.ENODATA, "No trace information for " + j.queueId);
				}

				if (getJDL) {
					final String jdl = commander.q_api.getJDL(j.queueId, true);
					if (jdl != null) {
						if (commander.bColour)
							commander.printOutln(ShellColor.jobStateRed() + jdl + ShellColor.reset());
						else
							commander.printOutln(jdl);

						commander.printOut("jdl", jdl);
					}
					else
						commander.setReturnCode(ErrNo.ENODATA, "JDL of " + j.queueId + " could not be retrieved");
				}
			}
	}

	private String printPriority(final JobStatus status, final int priority) {

		if (JobStatus.INSERTING == status || JobStatus.WAITING == status) {
			if (commander.bColour) {
				String cTag = "";
				if (priority <= 0)
					cTag = ShellColor.jobStateBlueError();
				else if (priority < 70)
					cTag = ShellColor.jobStateBlue();
				else
					cTag = ShellColor.jobStateGreen();
				return cTag + padLeft(String.valueOf(priority), 3) + ShellColor.reset();
			}
			return padLeft(String.valueOf(priority), 3);
		}
		return "___";
	}

	private String abbrvStatus(final JobStatus status) {
		if (status == null)
			return padLeft("NUL", 3);

		String e = "";

		if (commander.bColour) {
			switch (status) {
				case KILLED:
					return ShellColor.jobStateRed() + padLeft("  K", 3) + ShellColor.reset();
				case RUNNING:
					return ShellColor.jobStateGreen() + padLeft("  R", 3) + ShellColor.reset();
				case STARTED:
					return ShellColor.jobStateGreen() + padLeft(" ST", 3) + ShellColor.reset();
				case DONE:
					return padLeft("  D", 3);
				case DONE_WARN:
					return padLeft(" DW", 3);
				case WAITING:
					return ShellColor.jobStateBlue() + padLeft("  W", 3) + ShellColor.reset();
				case OVER_WAITING:
					return padLeft(" OW", 3);
				case INSERTING:
					return ShellColor.jobStateYellow() + padLeft("  I", 3) + ShellColor.reset();
				case SPLIT:
					return padLeft("  S", 3);
				case SPLITTING:
					return padLeft(" SP", 3);
				case SAVING:
					return ShellColor.jobStateGreen() + padLeft(" SV", 3) + ShellColor.reset();
				case SAVED:
					return padLeft("SVD", 3);
				case ANY:
					return padLeft("ANY", 3); // shouldn't happen!
				case ASSIGNED:
					return padLeft("ASG", 3);
				case A_STAGED:
					return padLeft("AST", 3);
				case FORCEMERGE:
					return padLeft(" FM", 3);
				case IDLE:
					return padLeft("IDL", 3);
				case INTERACTIV:
					return padLeft("INT", 3);
				case MERGING:
					return padLeft("  M", 3);
				case SAVED_WARN:
					return padLeft(" SW", 3);
				case STAGING:
					return padLeft(" SG", 3);
				case TO_STAGE:
					return padLeft("TST", 3);
				case ERROR_A:
					e = " EA";
					break;
				case ERROR_E:
					e = " EE";
					break;
				case ERROR_I:
					e = " EI";
					break;
				case ERROR_IB:
					e = "EIB";
					break;
				case ERROR_M:
					e = " EM";
					break;
				case ERROR_RE:
					e = "ERE";
					break;
				case ERROR_S:
					e = " ES";
					break;
				case ERROR_SV:
					e = "ESV";
					break;
				case ERROR_V:
					e = " EV";
					break;
				case ERROR_VN:
					e = "EVN";
					break;
				case ERROR_VT:
					e = "EVT";
					break;
				case ERROR_SPLT:
					e = "ESP";
					break;
				case ERROR_W:
					e = " EW";
					break;
				case ERROR_VER:
					e = "EVE";
					break;
				case FAILED:
					e = " FF";
					break;
				case ZOMBIE:
					e = "  Z";
					break;
				case EXPIRED:
					e = " XP";
					break;
				case ERROR_EW:
					e = "EEW";
					break;
				case UPDATING:
					e = " UP";
					break;
				case FAULTY:
					e = "  F";
					break;
				case INCORRECT:
					e = "INC";
					break;
				default:
					break;
			}

			return ShellColor.jobStateRedError() + padLeft(e, 3) + ShellColor.reset();
		}

		switch (status) {
			case KILLED:
				return padLeft("  K", 3);
			case RUNNING:
				return padLeft("  R", 3);
			case STARTED:
				return padLeft(" ST", 3);
			case DONE:
				return padLeft("  D", 3);
			case DONE_WARN:
				return padLeft(" DW", 3);
			case WAITING:
				return padLeft("  W", 3);
			case OVER_WAITING:
				return padLeft(" OW", 3);
			case INSERTING:
				return padLeft("  I", 3);
			case SPLIT:
				return padLeft("  S", 3);
			case SPLITTING:
				return padLeft(" SP", 3);
			case SAVING:
				return padLeft(" SV", 3);
			case SAVED:
				return padLeft("SVD", 3);
			case ANY:
				return padLeft("ANY", 3); // shouldn't happen!
			case ASSIGNED:
				return padLeft("ASG", 3);
			case A_STAGED:
				return padLeft("AST", 3);
			case FORCEMERGE:
				return padLeft(" FM", 3);
			case IDLE:
				return padLeft("IDL", 3);
			case INTERACTIV:
				return padLeft("INT", 3);
			case MERGING:
				return padLeft("  M", 3);
			case SAVED_WARN:
				return padLeft(" SW", 3);
			case STAGING:
				return padLeft(" ST", 3);
			case TO_STAGE:
				return padLeft("TST", 3);
			case ERROR_A:
				e = " EA";
				break;
			case ERROR_E:
				e = " EE";
				break;
			case ERROR_I:
				e = " EI";
				break;
			case ERROR_IB:
				e = "EIB";
				break;
			case ERROR_M:
				e = " EM";
				break;
			case ERROR_RE:
				e = "ERE";
				break;
			case ERROR_S:
				e = " ES";
				break;
			case ERROR_SV:
				e = "ESV";
				break;
			case ERROR_V:
				e = " EV";
				break;
			case ERROR_VN:
				e = "EVN";
				break;
			case ERROR_VT:
				e = "EVT";
				break;
			case ERROR_SPLT:
				e = "ESP";
				break;
			case ERROR_W:
				e = " EW";
				break;
			case ERROR_VER:
				e = "EVE";
				break;
			case FAILED:
				e = " FF";
				break;
			case ZOMBIE:
				e = "  Z";
				break;
			case EXPIRED:
				e = " XP";
				break;
			case ERROR_EW:
				e = "EEW";
				break;
			case UPDATING:
				e = " UP";
				break;
			case FAULTY:
				e = "  F";
				break;
			case INCORRECT:
				e = "INC";
				break;
			default:
				break;
		}

		return padLeft(e, 3);
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("ps", "[-options]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-F l | -Fl | -L", "long output format"));
		commander.printOutln(
				helpOption("-f <flags|status>", "any number of (long or short) upper case job states, or 'a' for all, 'r' for running states, 'f' for failed, 'd' for done, 's' for queued"));
		commander.printOutln(helpOption("-u <userlist>"));
		commander.printOutln(helpOption("-s <sitelist>"));
		commander.printOutln(helpOption("-n <nodelist>"));
		commander.printOutln(helpOption("-m <masterjoblist>"));
		commander.printOutln(helpOption("-o <sortkey>"));
		commander.printOutln(helpOption("-j <jobidlist>"));
		commander.printOutln(helpOption("-l <query-limit>"));
		commander.printOutln(helpOption("-t <time-limit-start, time-limit-end>", "modified time between " +
				"time-limit-start and time-limit-end in 'hours'. eg: -t 12,6 will output the jobs that were modified between last 12h to last 6h"));

		commander.printOutln();
		commander.printOutln(helpOption("-M", "show only masterjobs"));
		commander.printOutln(helpOption("-X", "active jobs in extended format"));
		commander.printOutln(helpOption("-A", "select all owned jobs of you"));
		commander.printOutln(helpOption("-W", "select all jobs which are waiting for execution of you"));
		commander.printOutln(helpOption("-E", "select all jobs which are in error state of you"));
		commander.printOutln(helpOption("-a", "select jobs of all users"));
		commander.printOutln(helpOption("-b", "do only black-white output"));
		commander.printOutln(helpOption("-jdl <jobid>", "display the job jdl"));
		commander.printOutln(helpOption("-trace <jobid>", "display the job trace information"));
		commander.printOutln(helpOption("-id", "only list the matching job IDs, for batch processing (implies -b)"));
		commander.printOutln(helpOption("-q", "quiet, no user printable output, API function only"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return true;
	}

	/**
	 * Constructor needed for the command factory in commander
	 *
	 * @param commander
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandps(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			final OptionParser parser = new OptionParser();

			parser.accepts("F").withRequiredArg();
			parser.accepts("Fl");
			parser.accepts("L");

			parser.accepts("f").withRequiredArg().ofType(String.class);
			parser.accepts("u").withRequiredArg().ofType(String.class);
			parser.accepts("s").withRequiredArg().ofType(String.class);
			parser.accepts("n").withRequiredArg();
			parser.accepts("m").withRequiredArg();
			parser.accepts("o").withRequiredArg();
			parser.accepts("j").withRequiredArg();
			parser.accepts("l").withRequiredArg().ofType(Integer.class);
			parser.accepts("q").withOptionalArg();
			parser.accepts("t").withOptionalArg();

			parser.accepts("M");
			parser.accepts("X");
			parser.accepts("A");
			parser.accepts("W");
			parser.accepts("E");
			parser.accepts("a");
			parser.accepts("b");
			parser.accepts("jdl").withOptionalArg().ofType(Long.class);
			parser.accepts("trace").withOptionalArg().ofType(Long.class);
			parser.accepts("id");

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("s") && options.hasArgument("s")) {
				final StringTokenizer st = new StringTokenizer((String) options.valueOf("s"), ",");
				while (st.hasMoreTokens())
					sites.add(st.nextToken());

				states.add(JobStatus.ANY);
			}

			if (options.has("n") && options.hasArgument("n")) {
				final StringTokenizer st = new StringTokenizer((String) options.valueOf("n"), ",");
				while (st.hasMoreTokens())
					nodes.add(st.nextToken());
				states.add(JobStatus.ANY);
			}

			if (options.has("m") && options.hasArgument("m")) {
				final StringTokenizer st = new StringTokenizer((String) options.valueOf("m"), ",");

				while (st.hasMoreTokens())
					try {
						mjobs.add(Long.valueOf(st.nextToken()));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						// ignore
					}

				states.add(JobStatus.ANY);
			}

			if (options.has("j") && options.hasArgument("j")) {
				final StringTokenizer st = new StringTokenizer((String) options.valueOf("j"), ",");
				while (st.hasMoreTokens())
					try {
						jobid.add(Long.valueOf(st.nextToken()));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						// ignore
					}
			}

			if (options.has("t") && options.hasArgument("t")) {
				final StringTokenizer st = new StringTokenizer((String) options.valueOf("t"), ",");
				int startTime = 0;
				int endTime = 0;
				// Only start time and end time(optional) should be passed
				if (st.countTokens() == 1 || st.countTokens() == 2) {
					try {
						startTime = Integer.parseInt(st.nextToken());
					} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
						commander.setReturnCode(ErrNo.EINVAL, "Invalid argument provided as the start time." +
								" Start time should be a number.");
						setArgumentsOk(false);
					}
					// If there is one more token, use it as the end time
					if (st.countTokens() == 1) {
						try {
							endTime = Integer.parseInt(st.nextToken());
						} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
							commander.setReturnCode(ErrNo.EINVAL, "Invalid argument provided for the end time." +
									" End time should be a number.");
							setArgumentsOk(false);
						}
					}
					// Ensure the provided numbers are positive
					if (!(startTime >= 0) || !(endTime >= 0)) {
						commander.setReturnCode(ErrNo.EINVAL, "Invalid argument for time interval" + options.valueOf("t"));
						setArgumentsOk(false);
					}
					// If the time interval is provided in the wrong order, swap them
					if (startTime < endTime) {
						int tempTime = startTime;
						startTime = endTime;
						endTime = tempTime;
					}
					ArrayList<Object> mtimeFilter = new ArrayList<>();
					mtimeFilter.add(startTime);
					mtimeFilter.add(endTime);
					filters.put(GetPS.PsFilters.mtime, mtimeFilter);
				} else {
					commander.setReturnCode(ErrNo.EINVAL, "Invalid number of arguments for time interval" + options.valueOf("t"));
					setArgumentsOk(false);
				}
			}

			if (options.has("X")) {
				bL = true;

				states.clear();

				states.addAll(flag_r());
				states.addAll(flag_s());
			}

			if (options.has("E")) {
				states.addAll(flag_f());
				users.add(commander.getUsername());
			}

			if (options.has("W")) {
				states.addAll(flag_s());
				users.add(commander.getUsername());
			}

			if (options.has("f") && options.hasArgument("f")) {
				states.clear();

				decodeFlagsAndStates((String) options.valueOf("f"));
			}

			if (options.has("u") && options.hasArgument("u")) {
				final StringTokenizer st = new StringTokenizer((String) options.valueOf("u"), ",");
				while (st.hasMoreTokens())
					users.add(st.nextToken());
			}

			if (options.has("l") && options.hasArgument("l"))
				try {
					final int lim = ((Integer) options.valueOf("l")).intValue();
					if (lim > 0)
						limit = lim;
				}
				catch (@SuppressWarnings("unused") final NumberFormatException e) {
					// ignore
				}

			if (options.has("Fl") || options.has("L") || (options.has("F") && "l".equals(options.valueOf("F"))))
				bL = true;

			if ((options.has("o") && options.hasArgument("o")))
				orderByKey = (String) options.valueOf("o");

			//
			// case 'M':
			// st_masterjobs = "\\\\0";

			if (options.has("A")) {
				states.clear();
				states.add(JobStatus.ANY);
				users.add(commander.getUsername());
			}

			if (options.has("M")) {
				mjobs.clear();
				mjobs.add(Long.valueOf(0));
			}

			if (options.has("a")) {
				users.clear();
				users.add("%");
			}

			if (options.has("jdl")) {
				getJDL = true;
				if (options.hasArgument("jdl")) {
					jobid.clear();
					try {
						jobid.add((Long) options.valueOf("jdl"));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException e) {
						commander.setReturnCode(ErrNo.EILSEQ, "Illegal job ID " + options.valueOf("jdl"));
					}
				}
				else if (jobid.size() == 0)
					commander.setReturnCode(ErrNo.EINVAL, "No job IDs to get the JDL for");
			}

			if (options.has("trace")) {
				getTrace = true;
				if (options.hasArgument("trace")) {
					jobid.clear();
					try {
						jobid.add((Long) options.valueOf("trace"));
					}
					catch (@SuppressWarnings("unused") final NumberFormatException e) {
						commander.setReturnCode(ErrNo.EILSEQ, "Illegal job ID " + options.valueOf("trace"));
					}
				}
				else if (jobid.size() == 0)
					commander.setReturnCode(ErrNo.EINVAL, "No job IDs to get the trace for");
			}

			if (jobid.size() > 0) {
				states.add(JobStatus.ANY);
				users.add("%");
			}

			bIDOnly = options.has("id");

			if (options.has("b") || bIDOnly)
				commander.bColour = false;
		}
		catch (

		final OptionException e) {
			commander.setReturnCode(ErrNo.EINVAL, e.getMessage());
			setArgumentsOk(false);
		}
	}

	private void decodeFlagsAndStates(final String line) {

		if (line == null || line.length() < 1)
			return;

		boolean all = false;

		if (line.startsWith("-")) {
			if ((line.length() == 1) || ("-a".equals(line)))
				all = true;
			else {
				final char[] flags = line.toCharArray();
				for (final char f : flags)
					switch (f) {
						case 'a':
							all = true;
							break;
						case 'r':
							states.addAll(flag_r());
							break;
						case 'q':
							states.addAll(flag_q());
							break;
						case 'f':
							states.addAll(flag_f());
							break;
						case 'd':
							states.addAll(flag_d());
							break;
						case 't':
							states.addAll(flag_t());
							break;
						case 's':
							states.addAll(flag_s());
							break;
						default:
							// ignore any other flag
							break;
					}
			}
		}
		else {
			final StringTokenizer st = new StringTokenizer(line, ",");
			while (st.hasMoreTokens()) {
				final String o = st.nextToken().toUpperCase();

				if (o.length() < 1)
					continue;

				if ("%".equals(o) || "ANY".equals(o)) {
					all = true;
					break;
				}

				if ("ERROR_ALL".equals(o) || "ERROR_ANY".equals(o)) {
					states.addAll(JobStatus.errorneousStates());
					continue;
				}

				if ("DONE_ANY".equals(o)) {
					states.addAll(JobStatus.doneStates());
					continue;
				}

				JobStatus status = JobStatus.getStatus(o);

				if (status == null) {
					final String alternativeStatus = SHORT_TO_LONG_STATUS.get(o);

					if (alternativeStatus != null)
						status = JobStatus.getStatus(alternativeStatus);
				}

				if (status != null)
					states.add(status);

				// System.out.println("added status: " + states);
			}
		}

		if (all) {
			states.clear();
			states.add(JobStatus.ANY);
		}
	}

	private static Set<JobStatus> flag_f() {
		return JobStatus.errorneousStates();
	}

	private static Set<JobStatus> flag_d() {
		return JobStatus.doneStates();
	}

	private static Set<JobStatus> flag_t() {
		return JobStatus.finalStates();
	}

	private static Set<JobStatus> flag_s() {
		return JobStatus.waitingStates();
	}

	private static Set<JobStatus> flag_q() {
		return JobStatus.queuedStates();
	}

	private static Set<JobStatus> flag_r() {
		return JobStatus.runningStates();
	}
}
