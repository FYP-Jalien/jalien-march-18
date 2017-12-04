package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.logging.Level;

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
	private int getJDL = 0;

	/**
	 * id of the job to get the trace for
	 */
	private int getTrace = 0;

	private final Set<JobStatus> states = new HashSet<>();

	private final Set<String> users = new LinkedHashSet<>();

	private final Set<String> sites = new LinkedHashSet<>();

	private final Set<String> nodes = new LinkedHashSet<>();

	private final Set<Integer> mjobs = new LinkedHashSet<>();

	private final Set<Integer> jobid = new LinkedHashSet<>();

	private String orderByKey = "queueId";

	private int limit = 0;

	@Override
	public void run() {
		logger.log(Level.INFO, "Starting ps");

		if (out.isRootPrinter()) {
			out.nextResult();
			logger.log(Level.INFO, "getJDL " + String.valueOf(getJDL));

			if (getJDL != 0) {
				final String jdl = commander.q_api.getJDL(getJDL);
				if (jdl != null)
					if (bColour)
						out.setField("value ", ShellColor.jobStateRed() + jdl + ShellColor.reset());
					else
						out.setField("value ", jdl);
			}
			else
				if (getTrace > 0) {
					final String tracelog = commander.q_api.getTraceLog(getTrace);
					logger.log(Level.FINE, "tracelog " + tracelog);

					if (tracelog != null)
						if (bColour)
							out.setField("value ", ShellColor.jobStateBlue() + tracelog + ShellColor.reset());
						else
							out.setField("value ", tracelog);
					out.setField("message: ", "not implemented yet");
				}
				else {
					if (users.size() == 0)
						users.add(commander.getUsername());

					final List<Job> ps = commander.q_api.getPS(states, users, sites, nodes, mjobs, jobid, orderByKey, limit);
					logger.log(Level.INFO, "ps " + ps);

					if (ps != null)
						for (final Job j : ps) {
							out.nextResult();
							final String owner = (j.getOwner() != null) ? j.getOwner() : "";

							final String jId = bColour ? ShellColor.bold() + j.queueId + ShellColor.reset() : String.valueOf(j.queueId);

							final String name = (j.name != null) ? j.name.substring(j.name.lastIndexOf('/') + 1) : "";

							if (bL) {
								final String site = (j.site != null) ? j.site : "";
								final String node = (j.node != null) ? j.node : "";
								out.setField("owner ", String.valueOf(owner));
								out.setField("id ", jId.toString());
								out.setField("site ", String.valueOf(site));
								out.setField("node", String.valueOf(node));
								out.setField("status", " " + j.status());
								out.setField("name", String.valueOf(name));
							}
							else {
								out.setField("owner ", String.valueOf(owner));
								out.setField("id ", jId.toString());
								out.setField("priority ", printPriority(j.status(), j.priority));
								out.setField("status ", " " + j.status());
								out.setField("name ", String.valueOf(name));
							}
						}
				}

		}
		else
			if (getJDL != 0) {
				final String jdl = commander.q_api.getJDL(getJDL);
				if (jdl != null)
					if (bColour)
						out.printOutln(ShellColor.jobStateRed() + jdl + ShellColor.reset());
					else
						out.printOutln(jdl);
			}
			else
				if (getTrace > 0) {
					final String tracelog = commander.q_api.getTraceLog(getTrace);
					logger.log(Level.FINE, "tracelog " + tracelog);

					if (tracelog != null)
						if (bColour)
							out.printOutln(ShellColor.jobStateBlue() + tracelog + ShellColor.reset());

					out.printOutln("--- not implemented yet ---");

				}
				else {
					if (users.size() == 0)
						users.add(commander.getUsername());

					final List<Job> ps = commander.q_api.getPS(states, users, sites, nodes, mjobs, jobid, orderByKey, limit);
					logger.log(Level.INFO, "ps " + ps);

					if (ps != null)
						for (final Job j : ps) {

							final String owner = (j.getOwner() != null) ? j.getOwner() : "";

							final String jId = bColour ? ShellColor.bold() + j.queueId + ShellColor.reset() : String.valueOf(j.queueId);

							final String name = (j.name != null) ? j.name.substring(j.name.lastIndexOf('/') + 1) : "";

							if (bL) {
								final String site = (j.site != null) ? j.site : "";
								final String node = (j.node != null) ? j.node : "";
								out.printOutln(padLeft(String.valueOf(owner), 10) + padSpace(4) + padLeft(jId, 10) + padSpace(2) + printPriority(j.status(), j.priority) + padSpace(2)
										+ padLeft(String.valueOf(site), 38) + padSpace(2) + padLeft(String.valueOf(node), 40) + padSpace(2) + abbrvStatus(j.status()) + padSpace(2)
										+ padLeft(String.valueOf(name), 30));
							}
							else
								out.printOutln(padLeft(String.valueOf(owner), 10) + padSpace(1) + padLeft(jId, 10) + padSpace(2) + printPriority(j.status(), j.priority) + padSpace(2)
										+ abbrvStatus(j.status()) + padSpace(2) + padLeft(String.valueOf(name), 32));

						}
				}
	}

	private String printPriority(final JobStatus status, final int priority) {

		if (JobStatus.INSERTING == status || JobStatus.WAITING == status) {
			if (bColour) {
				String cTag = "";
				if (priority <= 0)
					cTag = ShellColor.jobStateBlueError();
				else
					if (priority < 70)
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

		if (bColour) {
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
				e = " EW";
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
			e = " EW";
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

		out.printOutln();
		out.printOutln(helpUsage("ps", "[-options]"));
		out.printOutln(helpStartOptions());
		out.printOutln(helpOption("-F l | -Fl | -L", "long output format"));
		out.printOutln(helpOption("-f <flags|status>"));
		out.printOutln(helpOption("-u <userlist>"));
		out.printOutln(helpOption("-s <sitelist>"));
		out.printOutln(helpOption("-n <nodelist>"));
		out.printOutln(helpOption("-m <masterjoblist>"));
		out.printOutln(helpOption("-o <sortkey>"));
		out.printOutln(helpOption("-j <jobidlist>"));
		out.printOutln(helpOption("-l <query-limit>"));

		out.printOutln();
		out.printOutln(helpOption("-M", "show only masterjobs"));
		out.printOutln(helpOption("-X", "active jobs in extended format"));
		out.printOutln(helpOption("-A", "select all owned jobs of you"));
		out.printOutln(helpOption("-W", "select all jobs which are waiting for execution of you"));
		out.printOutln(helpOption("-E", "select all jobs which are in error state of you"));
		out.printOutln(helpOption("-a", "select jobs of all users"));
		out.printOutln(helpOption("-b", "do only black-white output"));
		out.printOutln(helpOption("-jdl <jobid>", "display the job jdl"));
		out.printOutln(helpOption("-trace <jobid> <tag>*", "display the job trace information (not working yet!)")); // TODO:
		out.printOutln();

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
	 * @param out
	 *
	 * @param alArguments
	 *            the arguments of the command
	 * @throws OptionException
	 */
	public JAliEnCommandps(final JAliEnCOMMander commander, final UIPrintWriter out, final ArrayList<String> alArguments) throws OptionException {
		super(commander, out, alArguments);

		try {

			final OptionParser parser = new OptionParser();

			parser.accepts("F").withRequiredArg();
			parser.accepts("Fl");
			parser.accepts("L");

			parser.accepts("f").withRequiredArg();
			parser.accepts("u").withRequiredArg();
			parser.accepts("s").withRequiredArg();
			parser.accepts("n").withRequiredArg();
			parser.accepts("m").withRequiredArg();
			parser.accepts("o").withRequiredArg();
			parser.accepts("j").withRequiredArg();
			parser.accepts("l").withRequiredArg();
			parser.accepts("q").withRequiredArg();

			parser.accepts("M");
			parser.accepts("X");
			parser.accepts("A");
			parser.accepts("W");
			parser.accepts("E");
			parser.accepts("a");
			parser.accepts("b");
			parser.accepts("jdl").withRequiredArg();
			parser.accepts("trace").withRequiredArg();

			final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

			if (options.has("jdl") && options.hasArgument("jdl"))
				try {
					getJDL = Integer.parseInt((String) options.valueOf("jdl"));
				} catch (@SuppressWarnings("unused") final NumberFormatException e) {
					out.printErrln("Illegal job ID " + options.valueOf("jdl"));
					getJDL = -1;
				}
			else
				if (options.has("trace") && options.hasArgument("trace"))
					try {
						getTrace = Integer.parseInt((String) options.valueOf("trace"));
					} catch (@SuppressWarnings("unused") final NumberFormatException e) {
						out.printErrln("Illegal job ID " + options.valueOf("trace"));
						getTrace = -1;
					}
				else {

					if (options.has("f") && options.hasArgument("f"))
						decodeFlagsAndStates((String) options.valueOf("f"));

					if (options.has("u") && options.hasArgument("u")) {
						final StringTokenizer st = new StringTokenizer((String) options.valueOf("u"), ",");
						while (st.hasMoreTokens())
							users.add(st.nextToken());
					}

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
								mjobs.add(Integer.valueOf(st.nextToken()));
							} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
								// ignore
							}

						states.add(JobStatus.ANY);
					}

					if (options.has("j") && options.hasArgument("j")) {
						final StringTokenizer st = new StringTokenizer((String) options.valueOf("j"), ",");
						while (st.hasMoreTokens())
							try {
								jobid.add(Integer.valueOf(st.nextToken()));
							} catch (@SuppressWarnings("unused") final NumberFormatException nfe) {
								// ignore
							}
						states.add(JobStatus.ANY);
						users.add("%");
					}

					if (options.has("l") && options.hasArgument("l"))
						try {
							final int lim = Integer.parseInt((String) options.valueOf("l"));
							if (lim > 0)
								limit = lim;
						} catch (@SuppressWarnings("unused") final NumberFormatException e) {
							// ignore
						}

					if (options.has("X")) {
						bL = true;
						states.addAll(flag_r());
						states.addAll(flag_s());
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
					if (options.has("E")) {
						states.addAll(flag_f());
						users.add(commander.getUsername());
					}
					if (options.has("W")) {
						states.addAll(flag_s());
						users.add(commander.getUsername());
					}

					if (options.has("M")) {
						mjobs.clear();
						mjobs.add(Integer.valueOf(0));
					}

					if (options.has("a")) {
						users.clear();
						users.add("%");
					}
				}

			if (options.has("b"))
				bColour = false;

		} catch (final OptionException e) {
			printHelp();
			throw e;
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

				final JobStatus status = JobStatus.getStatus(o);

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
