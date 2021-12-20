package alien.shell.commands;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import alien.api.taskQueue.CE;
import alien.shell.ErrNo;
import alien.taskQueue.Job;
import alien.taskQueue.JobStatus;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * @author ron
 * @since June 9, 2011
 */
public class JAliEnCommandmasterjob extends JAliEnBaseCommand {
	/**
	 * marker for -M argument
	 */
	private boolean bPrintId = false;

	/**
	 * marker for -M argument
	 */
	private boolean bPrintSite = false;

	private final List<Long> jobIDs = new ArrayList<>();

	private final List<Long> id = new ArrayList<>();

	private Set<JobStatus> status = new HashSet<>();

	private final List<String> sites = new ArrayList<>();

	private List<CE> ces = null;

	@Override
	public void run() {
		final List<Job> subjobstates = new ArrayList<>();

		for (final Long jobId : jobIDs) {
			final Job j = commander.q_api.getJob(jobId.longValue());

			if (j != null) {
				commander.printOutln("Job " + j.queueId + " is in status: " + j.status());

				final List<Job> subjobs = commander.q_api.getMasterJobStatus(j.queueId, status, id, sites);
				if (subjobs != null)
					subjobstates.addAll(subjobs);
			}
			else
				commander.printErrln("Could not load the details of " + jobId);
		}

		final HashMap<String, List<Job>> stateCount = new HashMap<>();

		ArrayList<String> sitesIn = new ArrayList<>();
		ArrayList<JobStatus> statesIn = new ArrayList<>();

		final ArrayList<JobStatus> allStates = new ArrayList<>();
		final ArrayList<String> allSites = new ArrayList<>();

		if (subjobstates.size() > 0) {
			commander.printOutln("Subjobs aggregated by state:");

			for (final Job sj : subjobstates) {
				// count the states the subjobs have

				String key = sj.status().toString();

				if (bPrintSite) {
					final String site = sj.execHost != null && !sj.execHost.isBlank() ? sj.execHost : "";

					key += "/" + sj.execHost;

					if (sitesIn.size() <= 0)
						if (!allSites.contains(site))
							allSites.add(site);
				}

				List<Job> jobs = stateCount.get(key);

				if (jobs == null) {
					jobs = new ArrayList<>();
					stateCount.put(key, jobs);
				}

				jobs.add(sj);

				if (statesIn.size() <= 0 && !allStates.contains(sj.status()))
					allStates.add(sj.status());
			}
			if (statesIn.size() <= 0)
				statesIn = allStates;

			if (sitesIn.size() <= 0)
				sitesIn = allSites;

			//
			if (sitesIn.size() > 0)
				for (final String site : sitesIn)
					printSubJobs(stateCount, statesIn, site);
			else
				printSubJobs(stateCount, statesIn, null);

			commander.printOutln();
			commander.printOutln("In total, there are " + subjobstates.size() + " subjobs" + (jobIDs.size() > 1 ? " in " + jobIDs.size() + " masterjobs" : ""));
		}
	}

	private String getCEName(final String hostname) {
		if (ces == null) {
			ces = commander.c_api.getCEs(null);

			if (ces == null)
				ces = new ArrayList<>(0);
		}

		for (final CE ce : ces) {
			if (ce.host.equals(hostname))
				return ce.ceName;
		}

		return hostname;
	}

	private void printSubJobs(final HashMap<String, List<Job>> stateCount, final List<JobStatus> showStatus, final String site) {
		String key = "";

		if (site != null && site.length() > 0)
			key = "/" + site;

		for (final JobStatus state : showStatus) {
			final List<Job> subjobs = stateCount.get(state.toString() + key);

			if (subjobs != null && subjobs.size() > 0) {
				final StringBuilder ret = new StringBuilder();

				ret.append(padSpace(16)).append("Subjobs in ").append(state);
				if (bPrintSite)
					ret.append(" (").append(getCEName(site)).append(")");

				ret.append(": ").append(subjobs.size());

				if (bPrintId) {
					ret.append(" (ids: ");

					boolean first = true;

					for (final Job sj : subjobs) {
						if (!first)
							ret.append(',');
						else
							first = false;

						ret.append(sj.queueId);
					}

					ret.append(')');
				}

				commander.printOutln(ret.toString());
			}
		}
	}

	/**
	 * printout the help info
	 */
	@Override
	public void printHelp() {
		commander.printOutln();
		commander.printOutln(helpUsage("masterjob", "<jobIDs> [-options]"));
		commander.printOutln(helpStartOptions());
		commander.printOutln(helpOption("-status <status>", "display only the subjobs with that status"));
		commander.printOutln(helpOption("-id <id>", "display only the subjobs with that id"));
		commander.printOutln(helpOption("-site <id>", "display only the subjobs on that site"));
		commander.printOutln(helpOption("-printid", "print also the id of all the subjobs"));
		commander.printOutln(helpOption("-printsite", "split the number of jobs according to the execution site"));
		commander.printOutln();
	}

	/**
	 * cat cannot run without arguments
	 *
	 * @return <code>false</code>
	 */
	@Override
	public boolean canRunWithoutArguments() {
		return false;
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
	public JAliEnCommandmasterjob(final JAliEnCOMMander commander, final List<String> alArguments) throws OptionException {
		super(commander, alArguments);

		try {
			if (alArguments.size() > 0) {
				final OptionParser parser = new OptionParser();

				parser.accepts("status").withRequiredArg();
				parser.accepts("id").withRequiredArg();
				parser.accepts("site").withRequiredArg();

				parser.accepts("printid");
				parser.accepts("printsite");

				final OptionSet options = parser.parse(alArguments.toArray(new String[] {}));

				if (options.has("status") && options.hasArgument("status")) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("status"), ",");
					while (st.hasMoreTokens()) {
						final String state = st.nextToken();

						if ("ERROR_ALL".equals(state)) {
							status = JobStatus.errorneousStates();
							break;
						}

						status.add(JobStatus.getStatus(state));
					}
				}

				if (options.has("id") && options.hasArgument("id")) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("id"), ",");
					while (st.hasMoreTokens())
						try {
							id.add(Long.valueOf(st.nextToken()));
						}
						catch (@SuppressWarnings("unused") final NumberFormatException e) {
							// ignore
						}
				}

				if (options.has("site") && options.hasArgument("site")) {
					final StringTokenizer st = new StringTokenizer((String) options.valueOf("site"), ",");
					while (st.hasMoreTokens())
						sites.add(st.nextToken());
				}

				bPrintId = options.has("printid");
				bPrintSite = options.has("printsite");

				for (final Object o : options.nonOptionArguments()) {
					final StringTokenizer st = new StringTokenizer(o.toString(), ",; \r\n\t");

					while (st.hasMoreTokens()) {
						final String tok = st.nextToken();
						try {
							jobIDs.add(Long.valueOf(tok));
						}
						catch (@SuppressWarnings("unused") final NumberFormatException e) {
							commander.setReturnCode(ErrNo.EINVAL, "Incorrect job ID specification: " + tok);
							setArgumentsOk(false);
							return;
						}
					}
				}

				if (jobIDs.size() == 0)
					setArgumentsOk(false);
			}
			else
				setArgumentsOk(false);
		}
		catch (@SuppressWarnings("unused") final OptionException e) {
			setArgumentsOk(false);
		}
	}
}
